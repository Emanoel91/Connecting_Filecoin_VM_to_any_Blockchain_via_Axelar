import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

# --- Page Config: Tab Title & Icon ---
st.set_page_config(
    page_title="Connecting Filecoin VM to any Blockchain via Axelar",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg",
    layout="wide"
)

st.title("📡Monitoring Transfers & Users")

st.info(
    "📊Charts initially display data for a default time range. Select a custom range to view results for your desired period."
)

st.info(
    "⏳On-chain data retrieval may take a few moments. Please wait while the results load."
)

# --- Snowflake Connection ---
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse="SNOWFLAKE_LEARNING_WH",
    database="AXELAR",
    schema="PUBLIC"
)

# --- Time Frame & Period Selection ---
timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2024-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))

# --- Query Functions ---
@st.cache_data
def load_recent_transfers(start_date, end_date):
    query = f"""
        WITH axelar_services AS (
            SELECT created_at,
                   LOWER(data:send:original_source_chain) AS source_chain,
                   LOWER(data:send:original_destination_chain) AS destination_chain,
                   sender_address AS user,
                   data:send:amount * data:link:price AS amount,
                   data:send:fee_value AS fee,
                   id,
                   'Token Transfers' AS service,
                   data:link:asset AS asset
            FROM axelar.axelscan.fact_transfers
            WHERE (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed'
              AND simplified_status = 'received'

            UNION ALL

            SELECT created_at,
                   LOWER(data:call:chain) AS source_chain,
                   LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                   data:call:transaction:from AS user,
                   data:value AS amount,
                   COALESCE(
                       (data:gas:gas_used_amount) * (data:gas_price_rate:source_token.token_price.usd),
                       TRY_CAST(data:fees:express_fee_usd::float AS FLOAT)
                   ) AS fee,
                   id,
                   'GMP' AS service,
                   data:approved:returnValues:symbol AS asset
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed'
              AND simplified_status = 'received'
        )

        SELECT created_at AS "⏰Date",
               user AS "👥Asset Sender",
               source_chain || '➡' || destination_chain AS "🔀Path",
               CASE 
                   WHEN amount IS NULL THEN 'No Volume'
                   ELSE TO_VARCHAR(ROUND(amount, 1))
               END AS "💰Amount ($USD)",
               ROUND(fee, 5) AS "💸Transfer Fee ($USD)",
               id AS "⛓ID"
        FROM axelar_services
        ORDER BY 1 DESC
        LIMIT 1000
    """
    return pd.read_sql(query, conn)

@st.cache_data
def load_whale_transfers(start_date, end_date):
    query = f"""
        WITH axelar_services AS (
            SELECT 
                created_at, 
                LOWER(data:send:original_source_chain) AS source_chain, 
                LOWER(data:send:original_destination_chain) AS destination_chain,
                sender_address AS user, 
                CASE 
                  WHEN TRY_CAST(TO_VARCHAR(data:send:amount) AS FLOAT) IS NOT NULL 
                       AND TRY_CAST(TO_VARCHAR(data:link:price) AS FLOAT) IS NOT NULL 
                  THEN TRY_CAST(TO_VARCHAR(data:send:amount) AS FLOAT) * TRY_CAST(TO_VARCHAR(data:link:price) AS FLOAT)
                  ELSE NULL
                END AS amount,
                TRY_CAST(TO_VARCHAR(data:send:fee_value) AS FLOAT) AS fee, 
                id, 
                'Token Transfers' AS service,
                data:link:asset AS asset
            FROM axelar.axelscan.fact_transfers
            WHERE 
                (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
                AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
                AND status = 'executed'
                AND simplified_status = 'received'

            UNION ALL

            SELECT 
                created_at, 
                LOWER(data:call:chain) AS source_chain, 
                LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                data:call:transaction:from AS user, 
                CASE 
                  WHEN TRY_CAST(TO_VARCHAR(data:value) AS FLOAT) IS NOT NULL 
                  THEN TRY_CAST(TO_VARCHAR(data:value) AS FLOAT)
                  ELSE NULL
                END AS amount,
                COALESCE(
                  CASE 
                    WHEN TRY_CAST(TO_VARCHAR(data:gas:gas_used_amount) AS FLOAT) IS NOT NULL 
                         AND TRY_CAST(TO_VARCHAR(data:gas_price_rate:source_token.token_price.usd) AS FLOAT) IS NOT NULL
                    THEN TRY_CAST(TO_VARCHAR(data:gas:gas_used_amount) AS FLOAT) * TRY_CAST(TO_VARCHAR(data:gas_price_rate:source_token.token_price.usd) AS FLOAT)
                    ELSE NULL
                  END,
                  TRY_CAST(TO_VARCHAR(data:fees:express_fee_usd) AS FLOAT)
                ) AS fee, 
                TO_VARCHAR(id) AS id, 
                'GMP' AS service, 
                data:approved:returnValues:symbol AS asset
            FROM axelar.axelscan.fact_gmp
            WHERE 
                (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
                AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
                AND status = 'executed'
                AND simplified_status = 'received'
        )

        SELECT 
          created_at::DATE AS "⏰Date", 
          user AS "🐳Asset Sender", 
          source_chain || '➡' || destination_chain AS "🔀Path", 
          ROUND(amount, 1) AS "💰Amount ($USD)", 
          ROUND(fee, 3) AS "💸Transfer Fee ($USD)", 
          id AS "⛓ID"
        FROM axelar_services
        WHERE amount IS NOT NULL AND amount > 100000
        ORDER BY 1 DESC
    """
    return pd.read_sql(query, conn)

@st.cache_data
def load_top_users_by_volume(start_date, end_date):
    query = f"""
    WITH axelar_services AS (
      SELECT 
        created_at, 
        LOWER(data:send:original_source_chain) AS source_chain, 
        LOWER(data:send:original_destination_chain) AS destination_chain,
        sender_address AS user, 
        CASE 
          WHEN TRY_CAST(TO_VARCHAR(data:send:amount) AS FLOAT) IS NOT NULL 
               AND TRY_CAST(TO_VARCHAR(data:link:price) AS FLOAT) IS NOT NULL 
          THEN TRY_CAST(TO_VARCHAR(data:send:amount) AS FLOAT) * TRY_CAST(TO_VARCHAR(data:link:price) AS FLOAT)
          ELSE NULL
        END AS amount,
        TRY_CAST(TO_VARCHAR(data:send:fee_value) AS FLOAT) AS fee, 
        id, 
        'Token Transfers' AS service,
        data:link:asset AS asset
      FROM axelar.axelscan.fact_transfers
      WHERE 
        (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
        AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
        AND status = 'executed'
        AND simplified_status = 'received'

      UNION ALL

      SELECT 
        created_at, 
        TO_VARCHAR(LOWER(data:call:chain)) AS source_chain, 
        TO_VARCHAR(LOWER(data:call:returnValues:destinationChain)) AS destination_chain,
        TO_VARCHAR(data:call:transaction:from) AS user, 
        CASE 
          WHEN TRY_CAST(TO_VARCHAR(data:value) AS FLOAT) IS NOT NULL 
          THEN TRY_CAST(TO_VARCHAR(data:value) AS FLOAT)
          ELSE NULL
        END AS amount,
        COALESCE(
          CASE 
            WHEN TRY_CAST(TO_VARCHAR(data:gas:gas_used_amount) AS FLOAT) IS NOT NULL 
                 AND TRY_CAST(TO_VARCHAR(data:gas_price_rate:source_token.token_price.usd) AS FLOAT) IS NOT NULL
            THEN TRY_CAST(TO_VARCHAR(data:gas:gas_used_amount) AS FLOAT) * TRY_CAST(TO_VARCHAR(data:gas_price_rate:source_token.token_price.usd) AS FLOAT)
            ELSE NULL
          END,
          TRY_CAST(TO_VARCHAR(data:fees:express_fee_usd) AS FLOAT)
        ) AS fee, 
        TO_VARCHAR(id) AS id, 
        'GMP' AS service, 
        data:approved:returnValues:symbol AS asset
      FROM axelar.axelscan.fact_gmp
      WHERE 
        (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
        AND status = 'executed'
        AND simplified_status = 'received'
        AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
    )
    SELECT 
      user AS "User", 
      round(sum(amount),1) AS "Volume of Transfers", 
      count(distinct id) as "Number of Transfers"
    FROM axelar_services
    WHERE amount IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
    """
    return pd.read_sql(query, conn)

@st.cache_data
def load_top_users_by_count(start_date, end_date):
    query = f"""
    WITH axelar_services AS (
      SELECT 
        created_at, 
        LOWER(data:send:original_source_chain) AS source_chain, 
        LOWER(data:send:original_destination_chain) AS destination_chain,
        sender_address AS user, 
        CASE 
          WHEN TRY_CAST(TO_VARCHAR(data:send:amount) AS FLOAT) IS NOT NULL 
               AND TRY_CAST(TO_VARCHAR(data:link:price) AS FLOAT) IS NOT NULL 
          THEN TRY_CAST(TO_VARCHAR(data:send:amount) AS FLOAT) * TRY_CAST(TO_VARCHAR(data:link:price) AS FLOAT)
          ELSE NULL
        END AS amount,
        TRY_CAST(TO_VARCHAR(data:send:fee_value) AS FLOAT) AS fee, 
        id, 
        'Token Transfers' AS service,
        data:link:asset AS asset
      FROM axelar.axelscan.fact_transfers
      WHERE 
        (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
        AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
        AND status = 'executed'
        AND simplified_status = 'received'

      UNION ALL

      SELECT 
        created_at, 
        TO_VARCHAR(LOWER(data:call:chain)) AS source_chain, 
        TO_VARCHAR(LOWER(data:call:returnValues:destinationChain)) AS destination_chain,
        TO_VARCHAR(data:call:transaction:from) AS user, 
        CASE 
          WHEN TRY_CAST(TO_VARCHAR(data:value) AS FLOAT) IS NOT NULL 
          THEN TRY_CAST(TO_VARCHAR(data:value) AS FLOAT)
          ELSE NULL
        END AS amount,
        COALESCE(
          CASE 
            WHEN TRY_CAST(TO_VARCHAR(data:gas:gas_used_amount) AS FLOAT) IS NOT NULL 
                 AND TRY_CAST(TO_VARCHAR(data:gas_price_rate:source_token.token_price.usd) AS FLOAT) IS NOT NULL
            THEN TRY_CAST(TO_VARCHAR(data:gas:gas_used_amount) AS FLOAT) * TRY_CAST(TO_VARCHAR(data:gas_price_rate:source_token.token_price.usd) AS FLOAT)
            ELSE NULL
          END,
          TRY_CAST(TO_VARCHAR(data:fees:express_fee_usd) AS FLOAT)
        ) AS fee, 
        TO_VARCHAR(id) AS id, 
        'GMP' AS service, 
        data:approved:returnValues:symbol AS asset
      FROM axelar.axelscan.fact_gmp
      WHERE 
        (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
        AND status = 'executed'
        AND simplified_status = 'received'
        AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
    )
    SELECT 
      user AS "User", 
      round(sum(amount),1) AS "Volume of Transfers", 
      count(distinct id) as "Number of Transfers"
    FROM axelar_services
    WHERE amount IS NOT NULL
    GROUP BY 1
    ORDER BY 3 DESC
    LIMIT 5
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
recent_transfers = load_recent_transfers(start_date, end_date)
whale_transfers = load_whale_transfers(start_date, end_date)
top_users_volume = load_top_users_by_volume(start_date, end_date)
top_users_count = load_top_users_by_count(start_date, end_date)

# --- Show Tables ---
st.markdown(
    """
    <div style="background-color:#0090ff; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">🖥️Monitoring Transfers</h2>
    </div>
    """,
    unsafe_allow_html=True
)
st.markdown("### 📋 Tracking of Cross-Chain Transfers (Last 1000 Txns in Default Time Range)")
st.dataframe(recent_transfers)

st.markdown(
    """
    <div style="background-color:#0090ff; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">📊Analysis of Users</h2>
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown("### 🐳 Whale Transfers (> $100K)")
st.dataframe(whale_transfers)

# --- Process Top Users for Charts ---
top_5_users_volume = top_users_volume.head(5).reset_index(drop=True)
top_5_users_volume['User'] = top_5_users_volume['User'].astype(str)
top_5_users_volume['User_short'] = top_5_users_volume['User'].apply(lambda x: x[:8] + "..." if len(x) > 10 else x)

top_5_users_count = top_users_count.head(5).reset_index(drop=True)
top_5_users_count['User'] = top_5_users_count['User'].astype(str)
top_5_users_count['User_short'] = top_5_users_count['User'].apply(lambda x: x[:8] + "..." if len(x) > 10 else x)

# --- Plots ---
fig_horizontal_volume = px.bar(
    top_5_users_volume.sort_values("Volume of Transfers"),
    x="Volume of Transfers",
    y="User_short",
    orientation="h",
    text="Volume of Transfers",
    title="🏆Top Users By Transfer Volume"
)
fig_horizontal_volume.update_traces(textposition="outside")
fig_horizontal_volume.update_layout(
    xaxis_title="$USD",
    yaxis_title="User",
    height=500
)

fig_horizontal_count = px.bar(
    top_5_users_count.sort_values("Number of Transfers"),
    x="Number of Transfers",
    y="User_short",
    orientation="h",
    text="Number of Transfers",
    title="🏆Top Users By Transfer Count"
)
fig_horizontal_count.update_traces(textposition="outside")
fig_horizontal_count.update_layout(
    xaxis_title="Txns count",
    yaxis_title="User",
    height=500
)

# --- Display Plots Side by Side ---
col1, col2 = st.columns(2)
col1.plotly_chart(fig_horizontal_volume, use_container_width=True)
col2.plotly_chart(fig_horizontal_count, use_container_width=True)
