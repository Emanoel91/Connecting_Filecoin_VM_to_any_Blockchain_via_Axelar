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
def load_transfer_kpis(start_date, end_date):
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
            WHERE (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed'
              AND simplified_status = 'received'

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
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed'
              AND simplified_status = 'received'
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
# --- Row 3, 4 -----------------------------------------------------
@st.cache_data
def load_transfer_metrics_over_time(start_date, end_date, timeframe):
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
            WHERE (data:send:original_source_chain = 'filecoin' OR data:send:original_destination_chain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed'
              AND simplified_status = 'received'

            UNION ALL

            SELECT 
                created_at, 
                LOWER(data:call:chain) AS source_chain, 
                LOWER(data:call:returnValues:destinationChain) AS destination_chain,
                data:call:transaction:from AS user, 
                data:value AS amount, 
                COALESCE((data:gas:gas_used_amount) * (data:gas_price_rate:source_token.token_price.usd),  
                         TRY_CAST(data:fees:express_fee_usd::FLOAT AS FLOAT)) AS fee,
                id,
                'GMP' AS service
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
              AND status = 'executed'
              AND simplified_status = 'received'
        )
        SELECT 
            DATE_TRUNC('{timeframe}', created_at) AS "Date", 
            service, 
            COUNT(DISTINCT source_chain || '➡' || destination_chain) AS "Number of Path",
            COUNT(DISTINCT user) AS "User Count", 
            COUNT(DISTINCT id) AS "Transfer Count", 
            ROUND(SUM(amount)) AS "Transfer Volume", 
            ROUND(SUM(fee), 1) AS "Transfer Fees", 
            ROUND(AVG(fee), 2) AS "Avg", 
            MEDIAN(fee) AS "Median", 
            ROUND(MAX(fee), 2) AS "Max"
        FROM axelar_services
        GROUP BY 1,2
        ORDER BY 1
    """
    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------
transfer_kpis = load_transfer_kpis(start_date, end_date)
transfer_metrics_df = load_transfer_metrics_over_time(start_date, end_date, timeframe)
# ------------------------------------------------------------------------------------------------------
# --- Row 1: KPI Metrics (Volume, Count, Users) -----------------------------------------------------------------------
if not transfer_kpis.empty:
    volume = int(transfer_kpis["Transfer Volume"].iloc[0])
    txns = int(transfer_kpis["Transfer Count"].iloc[0])
    users = int(transfer_kpis["User Count"].iloc[0])

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="💸 Volume of Transfers", value=f"{volume:,.0f} USD")
    with col2:
        st.metric(label="🚀 Number of Transfers", value=f"{txns:,} Txns")
    with col3:
        st.metric(label="👥 Number of Users", value=f"{users:,} Addresses")
else:
    st.warning("No transfer data available for the selected period.")

# --- Row 2: KPI Metrics (Fees, Avg Fee, Paths) -----------------------------------------------------------------------
if not transfer_kpis.empty:
    fees = float(transfer_kpis["Transfer Fees"].iloc[0])
    avg_fee = float(transfer_kpis["Avg"].iloc[0])
    paths = int(transfer_kpis["Number of Path"].iloc[0])

    col4, col5, col6 = st.columns(3)
    with col4:
        st.metric(label="⛽ Total Transfer Fees", value=f"{fees:,.0f} USD")
    with col5:
        st.metric(label="📊 Average Transfer Fees", value=f"{avg_fee:,.2f} USD")
    with col6:
        st.metric(label="🔀 Number of Paths", value=f"{paths} Paths")

# --- Row 3: Volume & Transfer Count ----------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    if not transfer_metrics_df.empty:
        fig1 = go.Figure()

        for service, color in zip(["GMP", "Token Transfers"], ["#ff8700", "#008afa"]):
            df_filtered = transfer_metrics_df[transfer_metrics_df["service"] == service]
            fig1.add_bar(
                x=df_filtered["Date"],
                y=df_filtered["Transfer Volume"],
                name=service,
                marker_color=color
            )

        total_volume = transfer_metrics_df.groupby("Date")["Transfer Volume"].sum().reset_index()
        fig1.add_trace(
            go.Scatter(
                x=total_volume["Date"],
                y=total_volume["Transfer Volume"],
                name="Total Transfer Volume",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="#00cc96", width=3, dash="dash")
            )
        )

        fig1.update_layout(
            title="Volume of Cross-Chain Transfers Over Time",
            barmode="stack",
            height=500,
            yaxis=dict(title="Transfer Volume (USD)"),
            yaxis2=dict(title="Total Volume", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0)
        )
        st.plotly_chart(fig1, use_container_width=True)

with col2:
    if not transfer_metrics_df.empty:
        fig2 = go.Figure()

        for service, color in zip(["GMP", "Token Transfers"], ["#ff8700", "#008afa"]):
            df_filtered = transfer_metrics_df[transfer_metrics_df["service"] == service]
            fig2.add_bar(
                x=df_filtered["Date"],
                y=df_filtered["Transfer Count"],
                name=service,
                marker_color=color
            )

        total_count = transfer_metrics_df.groupby("Date")["Transfer Count"].sum().reset_index()
        fig2.add_trace(
            go.Scatter(
                x=total_count["Date"],
                y=total_count["Transfer Count"],
                name="Total Transfer Count",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="#00cc96", width=3, dash="dash")
            )
        )

        fig2.update_layout(
            title="Number of Cross-Chain Transfers Over Time",
            barmode="stack",
            height=500,
            yaxis=dict(title="Transfer Count"),
            yaxis2=dict(title="Total Transfers", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0)
        )
        st.plotly_chart(fig2, use_container_width=True)

# --- Row 4: Fees & Users ---------------------------------------------------------------------------------------------
col3, col4 = st.columns(2)

with col3:
    if not transfer_metrics_df.empty:
        fig3 = go.Figure()

        for service, color in zip(["GMP", "Token Transfers"], ["#ff8700", "#008afa"]):
            df_filtered = transfer_metrics_df[transfer_metrics_df["service"] == service]
            fig3.add_bar(
                x=df_filtered["Date"],
                y=df_filtered["Transfer Fees"],
                name=service,
                marker_color=color
            )

        total_fees = transfer_metrics_df.groupby("Date")["Transfer Fees"].sum().reset_index()
        fig3.add_trace(
            go.Scatter(
                x=total_fees["Date"],
                y=total_fees["Transfer Fees"],
                name="Total Transfer Fees",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color="#00cc96", width=3, dash="dash")
            )
        )

        fig3.update_layout(
            title="Total Transfer Fees Over Time",
            barmode="stack",
            height=500,
            yaxis=dict(title="Fees (USD)"),
            yaxis2=dict(title="Total Fees", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0)
        )
        st.plotly_chart(fig3, use_container_width=True)

with col4:
    if not transfer_metrics_df.empty:
        fig4 = px.bar(
            transfer_metrics_df,
            x="Date",
            y="User Count",
            color="service",
            color_discrete_map={
                "GMP": "#ff8700",
                "Token Transfers": "#008afa"
            },
            title="Number of Users Over Time"
        )
        fig4.update_layout(
            height=500,
            barmode="stack",
            yaxis_title="User Count",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0)
        )
        st.plotly_chart(fig4, use_container_width=True)
