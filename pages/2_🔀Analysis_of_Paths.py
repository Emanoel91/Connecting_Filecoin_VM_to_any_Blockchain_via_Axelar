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
@st.cache_data
def load_transfer_paths_stats(start_date,end_date):
    query = """
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
            WHERE (data:send:original_source_chain='filecoin' OR data:send:original_destination_chain='filecoin')
              AND created_at::date >= {start_date}
              AND created_at::date <= {end_date}
              AND status = 'executed'
              AND simplified_status = 'received'
            UNION ALL
            SELECT created_at,
                   LOWER(data:call:chain) AS source_chain,
                   LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                   data:call:transaction:from AS user,
                   data:value AS amount,
                   COALESCE((data:gas:gas_used_amount * data:gas_price_rate:source_token.token_price.usd),
                            TRY_CAST(data:fees:express_fee_usd::float AS FLOAT)) AS fee,
                   id,
                   'GMP' AS service
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain='filecoin' OR data:call:returnValues:destinationChain='filecoin')
              AND status = 'executed'
              AND simplified_status = 'received'
              AND created_at::date >= {start_date}
              AND created_at::date <= {end_date}
        )
        SELECT source_chain || '➡' || destination_chain AS path,
               COUNT(DISTINCT user) AS user_count,
               COUNT(DISTINCT id) AS transfer_count,
               ROUND(SUM(amount)) AS transfer_volume,
               ROUND(SUM(fee)) AS transfer_fees,
               ROUND(AVG(fee), 2) AS avg_fee
        FROM axelar_services
        GROUP BY 1
        ORDER BY 3 DESC
    """.format(start_date=start_date, end_date=end_date)
    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------
transfer_paths_stats = load_transfer_paths_stats(start_date,end_date)
# ------------------------------------------------------------------------------------------------------
# --- Display Table with Index Starting from 1 ------------------------------------------------------------------------
st.dataframe(transfer_paths_stats.reset_index(drop=True).style.format({
    'user_count': '{:,.0f}',
    'transfer_count': '{:,.0f}',
    'transfer_volume': '{:,.0f}',
    'transfer_fees': '{:,.0f}',
    'avg_fee': '{:,.2f}'
}).set_table_styles([{
    'selector': 'thead th',
    'props': [('background-color', '#f5f5f5'), ('font-weight', 'bold')]
}]), use_container_width=True)

# --- Row 1: Horizontal Bar Charts (Top Stats) ------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    fig = px.bar(
        transfer_paths_stats.sort_values("transfer_volume", ascending=True),
        x="transfer_volume", y="path",
        text="transfer_volume",
        orientation="h",
        color="transfer_volume",
        color_continuous_scale="blues",
        title="Top Paths By Transfer Volume"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        transfer_paths_stats.sort_values("transfer_count", ascending=True),
        x="transfer_count", y="path",
        text="transfer_count",
        orientation="h",
        color="transfer_count",
        color_continuous_scale="greens",
        title="Top Paths By Transfer Count"
    )
    st.plotly_chart(fig, use_container_width=True)

with col3:
    fig = px.bar(
        transfer_paths_stats.sort_values("user_count", ascending=True),
        x="user_count", y="path",
        text="user_count",
        orientation="h",
        color="user_count",
        color_continuous_scale="purples",
        title="Top Paths By User Count"
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Row 2: Horizontal Bar Charts (Fees) -----------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    fig = px.bar(
        transfer_paths_stats.sort_values("transfer_fees", ascending=True),
        x="transfer_fees", y="path",
        text="transfer_fees",
        orientation="h",
        color="transfer_fees",
        color_continuous_scale="reds",
        title="Paths with the Highest Fees Paid"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        transfer_paths_stats.sort_values("transfer_fees", ascending=False),
        x="transfer_fees", y="path",
        text="transfer_fees",
        orientation="h",
        color="transfer_fees",
        color_continuous_scale="oranges",
        title="Paths with the Lowest Fees Paid"
    )
    st.plotly_chart(fig, use_container_width=True)

with col3:
    fig = px.bar(
        transfer_paths_stats.sort_values("avg_fee", ascending=True),
        x="avg_fee", y="path",
        text="avg_fee",
        orientation="h",
        color="avg_fee",
        color_continuous_scale="teal",
        title="Most Expensive Paths Based on Average Fees"
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Row 3: Pie Charts (Source/Destination Paths) --------------------------------------------------------------------
col1, col2 = st.columns(2)
source_df = transfer_paths_stats[transfer_paths_stats["path"].str.startswith("filecoin➡")]
dest_df = transfer_paths_stats[transfer_paths_stats["path"].str.endswith("➡filecoin")]

with col1:
    fig = px.pie(
        source_df,
        values="transfer_volume",
        names="path",
        title="Top Destination Chains By Transfer Volume"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.pie(
        source_df,
        values="transfer_count",
        names="path",
        title="Top Destination Chains By Transfer Count"
    )
    st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    fig = px.pie(
        dest_df,
        values="transfer_volume",
        names="path",
        title="Top Source Chains By Transfer Volume"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.pie(
        dest_df,
        values="transfer_count",
        names="path",
        title="Top Source Chains By Transfer Count"
    )
    st.plotly_chart(fig, use_container_width=True)
