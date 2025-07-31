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

st.title("🔎Overview of Transfers")

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
# --- Row1, 2: KPIs --------------
@st.cache_data
def load_transfer_overview(start_date, end_date, conn):
    query = f"""
        WITH axelar_services AS (
            SELECT 
                created_at, 
                LOWER(data:send:original_source_chain) AS source_chain, 
                LOWER(data:send:original_destination_chain) AS destination_chain,
                sender_address AS user, 
                data:send:amount * data:link:price AS amount, 
                data:send:fee_value AS fee, 
                id, 
                'Token Transfers' AS service
            FROM axelar.axelscan.fact_transfers
            WHERE (data:send:original_source_chain='filecoin' OR data:send:original_destination_chain='filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status='executed'
              AND simplified_status='received'

            UNION ALL

            SELECT 
                created_at, 
                LOWER(data:call:chain) AS source_chain, 
                LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                data:call:transaction:from AS user, 
                data:value AS amount, 
                (data:gas:gas_used_amount) * (data:gas_price_rate:source_token.token_price.usd) AS fee, 
                id,
                'GMP' AS service
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain='filecoin' OR data:call:returnValues:destinationChain='filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status='executed'
              AND simplified_status='received'
        )
        SELECT 
            COUNT(DISTINCT source_chain || '➡' || destination_chain) AS "Number of Path",
            COUNT(DISTINCT user) AS "User Count", 
            COUNT(DISTINCT id) AS "Transfer Count", 
            ROUND(SUM(amount)) AS "Transfer Volume", 
            ROUND(SUM(fee)) AS "Transfer Fees", 
            ROUND(AVG(fee), 2) AS "Avg"
        FROM axelar_services
    """
    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------
overview_df = load_transfer_overview(start_date, end_date)
# ------------------------------------------------------------------------------------------------------
# --- Row 1, 2 -----------
if not overview_df.empty:
    volume = int(overview_df["Transfer Volume"].iloc[0])
    txns = int(overview_df["Transfer Count"].iloc[0])
    users = int(overview_df["User Count"].iloc[0])
    fees = float(overview_df["Transfer Fees"].iloc[0])
    avg_fee = float(overview_df["Avg"].iloc[0])
    paths = int(overview_df["Number of Path"].iloc[0])

    # Row 1
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="💸 Volume of Transfers", value=f"{volume:,.0f} USD")
    with col2:
        st.metric(label="🚀 Number of Transfers", value=f"{txns:,} Txns")
    with col3:
        st.metric(label="👥 Number of Users", value=f"{users:,} Addresses")

    # Row 2
    col4, col5, col6 = st.columns(3)
    with col4:
        st.metric(label="⛽ Total Transfer Fees", value=f"{fees:,.0f} USD")
    with col5:
        st.metric(label="📊 Average Transfer Fees", value=f"{avg_fee:,.2f} USD")
    with col6:
        st.metric(label="🔀 Number of Paths", value=f"{paths} Paths")
else:
    st.warning("No transfer data available for the selected period.")
