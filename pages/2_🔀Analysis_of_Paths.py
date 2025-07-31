import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go

# --- Page Config: Tab Title & Icon ---
st.set_page_config(
    page_title="Connecting Filecoin VM to any Blockchain via Axelar",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg",
    layout="wide"
)

st.title("🔀Analysis of Paths")

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

# --- Query Functions ---------------------------------------------------------------------------------------
# --- Row 1: Cross-Chain Path Table ------------------------------------------------------------------
@st.cache_data
def load_transfer_paths_table(start_date, end_date):
    query = f"""
        WITH axelar_services AS (
            SELECT created_at,
                   LOWER(data:send:original_source_chain) AS source_chain,
                   LOWER(data:send:original_destination_chain) AS destination_chain,
                   sender_address AS user,
                   data:send:amount * data:link:price AS amount,
                   data:send:fee_value AS fee,
                   id,
                   'Token Transfers' AS service
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
                   'GMP' AS service
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed'
              AND simplified_status = 'received'
        )
        SELECT 
            source_chain || '➡' || destination_chain AS "🔀Path",
            COUNT(DISTINCT user) AS "👥User Count",
            COUNT(DISTINCT id) AS "🚀Transfer Count",
            ROUND(SUM(amount)) AS "💰Transfer Volume ($USD)",
            ROUND(SUM(fee)) AS "💸Transfer Fees ($USD)",
            ROUND(AVG(fee), 2) AS "📊Avg Fee ($USD)"
        FROM axelar_services
        GROUP BY 1
        ORDER BY 3 DESC
    """
    return pd.read_sql(query, conn)

# --- Row 2: Top 10 Paths by Volume and Count (Side-by-Side Charts) ----------------------------------
# --- Row 9: Pie Charts for Source Chain Distribution -------------------------------------------------
@st.cache_data
def load_source_volume(start_date, end_date):
    query = f"""
        WITH axelar_services AS (
            SELECT created_at,
                   LOWER(data:send:original_source_chain) AS source_chain,
                   LOWER(data:send:original_destination_chain) AS destination_chain,
                   sender_address AS user,
                   data:send:amount * data:link:price AS amount,
                   data:send:fee_value AS fee,
                   id
            FROM axelar.axelscan.fact_transfers
            WHERE (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed' AND simplified_status = 'received'
            UNION ALL
            SELECT created_at,
                   LOWER(data:call:chain) AS source_chain,
                   LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                   data:call:transaction:from AS user,
                   data:value AS amount,
                   (data:gas:gas_used_amount)*(data:gas_price_rate:source_token.token_price.usd) AS fee,
                   id
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed' AND simplified_status = 'received'
        )
        SELECT source_chain AS "Source Chain",
               ROUND(SUM(amount), 2) AS "Transfer Volume"
        FROM axelar_services
        WHERE destination_chain = 'filecoin' AND source_chain <> 'filecoin' AND amount IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
    """
    return pd.read_sql(query, conn)

@st.cache_data
def load_source_transfer_count(start_date, end_date):
    query = f"""
        WITH axelar_services AS (
            SELECT created_at,
                   LOWER(data:send:original_source_chain) AS source_chain,
                   LOWER(data:send:original_destination_chain) AS destination_chain,
                   sender_address AS user,
                   data:send:amount * data:link:price AS amount,
                   data:send:fee_value AS fee,
                   id
            FROM axelar.axelscan.fact_transfers
            WHERE (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed' AND simplified_status = 'received'
            UNION ALL
            SELECT created_at,
                   LOWER(data:call:chain) AS source_chain,
                   LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                   data:call:transaction:from AS user,
                   data:value AS amount,
                   (data:gas:gas_used_amount)*(data:gas_price_rate:source_token.token_price.usd) AS fee,
                   id
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed' AND simplified_status = 'received'
        )
        SELECT source_chain AS "Source Chain",
               COUNT(DISTINCT id) AS "Transfer Count"
        FROM axelar_services
        WHERE destination_chain = 'filecoin'
        GROUP BY 1
        ORDER BY 2 DESC
    """
    return pd.read_sql(query, conn)

@st.cache_data
def load_source_user_count(start_date, end_date):
    query = f"""
        WITH axelar_services AS (
            SELECT created_at,
                   LOWER(data:send:original_source_chain) AS source_chain,
                   LOWER(data:send:original_destination_chain) AS destination_chain,
                   sender_address AS user,
                   data:send:amount * data:link:price AS amount,
                   data:send:fee_value AS fee,
                   id
            FROM axelar.axelscan.fact_transfers
            WHERE (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed' AND simplified_status = 'received'
            UNION ALL
            SELECT created_at,
                   LOWER(data:call:chain) AS source_chain,
                   LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                   data:call:transaction:from AS user,
                   data:value AS amount,
                   (data:gas:gas_used_amount)*(data:gas_price_rate:source_token.token_price.usd) AS fee,
                   id
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed' AND simplified_status = 'received'
        )
        SELECT source_chain AS "Source Chain",
               COUNT(DISTINCT user) AS "User Count"
        FROM axelar_services
        WHERE destination_chain = 'filecoin'
        GROUP BY 1
        ORDER BY 2 DESC
    """
    return pd.read_sql(query, conn)


# --- Load Data ----------------------------------------------------------------------------------------
path_table_df = load_transfer_paths_table(start_date, end_date)
volume_pie_df = load_source_volume(start_date, end_date)
count_pie_df = load_source_transfer_count(start_date, end_date)
user_pie_df = load_source_user_count(start_date, end_date)
# ------------------------------------------------------------------------------------------------------
# --- Row1: Render Table with Index Starting from 1 -----------------------------------
st.markdown("### 🔎Tracking of the Cross-Chain Paths (Sorted by transfers count)")

if not path_table_df.empty:
    path_table_df.index = path_table_df.index + 1  # Start index from 1
    st.dataframe(path_table_df, use_container_width=True)
else:
    st.warning("No cross-chain path data available for the selected period.")

# --- Row2 --------------------------------------
# --- Display all three pie charts in a single row
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 💰 Volume of Transfers By Source Chain")
    if not volume_pie_df.empty:
        fig_vol_pie = px.pie(
            volume_pie_df,
            names="Source Chain",
            values="Transfer Volume",
            title="",
            hole=0.4
        )
        fig_vol_pie.update_layout(legend=dict(orientation="v", x=1.1, y=0.5))
        st.plotly_chart(fig_vol_pie, use_container_width=True)
    else:
        st.warning("No volume data available.")

with col2:
    st.markdown("### 🚀 Number of Transfers By Source Chain")
    if not count_pie_df.empty:
        fig_cnt_pie = px.pie(
            count_pie_df,
            names="Source Chain",
            values="Transfer Count",
            title="",
            hole=0.4
        )
        fig_cnt_pie.update_layout(legend=dict(orientation="v", x=1.1, y=0.5))
        st.plotly_chart(fig_cnt_pie, use_container_width=True)
    else:
        st.warning("No count data available.")

with col3:
    st.markdown("### 👥 Number of Users By Source Chain")
    if not user_pie_df.empty:
        fig_usr_pie = px.pie(
            user_pie_df,
            names="Source Chain",
            values="User Count",
            title="",
            hole=0.4
        )
        fig_usr_pie.update_layout(legend=dict(orientation="v", x=-0.2, y=0.5))
        st.plotly_chart(fig_usr_pie, use_container_width=True)
    else:
        st.warning("No user data available.")
