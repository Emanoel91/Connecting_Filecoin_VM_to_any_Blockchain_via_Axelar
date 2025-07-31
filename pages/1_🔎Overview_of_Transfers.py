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
            service as "Service", 
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

# --- Row 5, 6 ---------------------------------------
@st.cache_data
def load_transfer_summary_by_service(start_date, end_date):
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
                   TO_VARCHAR(LOWER(data:call:chain)) AS source_chain,
                   TO_VARCHAR(LOWER(data:call:returnValues:destinationChain)) AS destination_chain,
                   TO_VARCHAR(data:call:transaction:from) AS user,
                   data:value AS amount,
                   COALESCE(
                     ((data:gas:gas_used_amount) * (data:gas_price_rate:source_token.token_price.usd)),
                     TRY_CAST(data:fees:express_fee_usd::FLOAT AS FLOAT)
                   ) AS fee,
                   TO_VARCHAR(id) AS id,
                   'GMP' AS service
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND status = 'executed'
              AND simplified_status = 'received'
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT service AS "Service",
               COUNT(DISTINCT source_chain || '➡' || destination_chain) AS "Number of Path",
               COUNT(DISTINCT user) AS "User Count",
               COUNT(DISTINCT id) AS "Transfer Count",
               ROUND(SUM(amount), 1) AS "Transfer Volume",
               ROUND(SUM(fee), 1) AS "Transfer Fees",
               ROUND(AVG(fee), 2) AS "Avg",
               MEDIAN(fee) AS "Median",
               MAX(fee) AS "Max"
        FROM axelar_services
        GROUP BY 1
    """
    return pd.read_sql(query, conn)
# -- Row 7 ----------------------------------------------------
@st.cache_data
def load_directional_transfer_summary(start_date, end_date):
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
                   TO_VARCHAR(LOWER(data:call:chain)) AS source_chain,
                   TO_VARCHAR(LOWER(data:call:returnValues:destinationChain)) AS destination_chain,
                   TO_VARCHAR(data:call:transaction:from) AS user,
                   data:value AS amount,
                   COALESCE(
                     ((data:gas:gas_used_amount) * (data:gas_price_rate:source_token.token_price.usd)),
                     TRY_CAST(data:fees:express_fee_usd::FLOAT AS FLOAT)
                   ) AS fee,
                   TO_VARCHAR(id) AS id,
                   'GMP' AS service
            FROM axelar.axelscan.fact_gmp
            WHERE (data:call:chain = 'filecoin' OR data:call:returnValues:destinationChain = 'filecoin')
              AND status = 'executed'
              AND simplified_status = 'received'
              AND created_at::DATE BETWEEN '{start_date}' AND '{end_date}'
        )

        SELECT
            CASE
                WHEN source_chain = 'filecoin' THEN 'filecoin➡⛓'
                WHEN destination_chain = 'filecoin' THEN '⛓➡filecoin'
            END AS "Direction",
            COUNT(DISTINCT user) AS "User Count",
            COUNT(DISTINCT id) AS "Transfer Count",
            ROUND(SUM(amount)) AS "Transfer Volume",
            ROUND(SUM(fee), 1) AS "Transfer Fees",
            ROUND(AVG(fee), 2) AS "Avg"
        FROM axelar_services
        GROUP BY 1
    """
    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------
transfer_kpis = load_transfer_kpis(start_date, end_date)
transfer_metrics_df = load_transfer_metrics_over_time(start_date, end_date, timeframe)
transfer_summary_df = load_transfer_summary_by_service(start_date, end_date)
directional_df = load_directional_transfer_summary(start_date, end_date)
# ------------------------------------------------------------------------------------------------------
# --- Row 1: KPI Metrics (Volume, Count, Users) -----------------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#0090ff; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">📋Overview</h2>
    </div>
    """,
    unsafe_allow_html=True
)
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
st.markdown(
    """
    <div style="background-color:#0090ff; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">📊Transfers Over Time</h2>
    </div>
    """,
    unsafe_allow_html=True
)
col1, col2 = st.columns(2)

with col1:
    if not transfer_metrics_df.empty:
        fig1 = go.Figure()

        for service, color in zip(["GMP", "Token Transfers"], ["#ff8700", "#008afa"]):
            df_filtered = transfer_metrics_df[transfer_metrics_df["Service"] == service]
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
                yaxis="y",
                mode="lines+markers",
                line=dict(color="#000000", width=2)
            )
        )

        fig1.update_layout(
            title="Volume of Cross-Chain Transfers Over Time",
            barmode="stack",
            height=500,
            yaxis=dict(title="$USD"),
            yaxis2=dict(title="Total Volume", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0)
        )
        st.plotly_chart(fig1, use_container_width=True)

with col2:
    if not transfer_metrics_df.empty:
        fig2 = go.Figure()

        for service, color in zip(["GMP", "Token Transfers"], ["#ff8700", "#008afa"]):
            df_filtered = transfer_metrics_df[transfer_metrics_df["Service"] == service]
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
                yaxis="y",
                mode="lines+markers",
                line=dict(color="#000000", width=2)
            )
        )

        fig2.update_layout(
            title="Number of Cross-Chain Transfers Over Time",
            barmode="stack",
            height=500,
            yaxis=dict(title="Txns Count"),
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
            df_filtered = transfer_metrics_df[transfer_metrics_df["Service"] == service]
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
                yaxis="y",
                mode="lines+markers",
                line=dict(color="#000000", width=2)
            )
        )

        fig3.update_layout(
            title="Total Transfer Fees Over Time",
            barmode="stack",
            height=500,
            yaxis=dict(title="$USD"),
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
            color="Service",
            color_discrete_map={
                "GMP": "#ff8700",
                "Token Transfers": "#008afa"
            },
            title="Number of Users Over Time"
        )
        fig4.update_layout(
            height=500,
            barmode="stack",
            xaxis_title=" ",
            yaxis_title="User Count",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0) 
        )
        st.plotly_chart(fig4, use_container_width=True)

# --- Row 5: Donut - Volume + Clustered Bar - Tx & Users --------------------------------------------------------------
st.markdown(
    """
    <div style="background-color:#0090ff; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">💎Transfers By Service</h2>
    </div>
    """,
    unsafe_allow_html=True
)
col1, col2 = st.columns(2)

with col1:
    if not transfer_summary_df.empty:
        fig1 = go.Figure(
            data=[
                go.Pie(
                    labels=transfer_summary_df["Service"],
                    values=transfer_summary_df["Transfer Volume"],
                    hole=0.5,
                    marker_colors=["#008afa" if s == "Token Transfers" else "#ff8700" for s in transfer_summary_df["Service"]],
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>%{value} USD"
                )
            ]
        )
        fig1.update_layout(
            title="Total Volume of Transfers By Service",
            height=500,
            legend=dict(orientation="v", x=1.05, y=0.5)
        )
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.warning("No data for volume distribution.")

with col2:
    if not transfer_summary_df.empty:
        fig2 = go.Figure(data=[
            go.Bar(
                x=transfer_summary_df["Service"],
                y=transfer_summary_df["Transfer Count"],
                name="Transfer Count",
                # -- marker_color=["#008afa" if s == "Token Transfers" else "#ff8700" for s in transfer_summary_df["Service"]],
            ),
            go.Bar(
                x=transfer_summary_df["Service"],
                y=transfer_summary_df["User Count"],
                name="User Count",
                # -- marker_color=["#004d99" if s == "Token Transfers" else "#cc6a00" for s in transfer_summary_df["Service"]],
            )
        ])
        fig2.update_layout(
            barmode="group",
            title="Total Number of Transfers & Users",
            yaxis_title="Count",
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0)
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No data for transfer/user counts.")

# --- Row 6: Donut - Fees + Bar - Avg Fees ---------------------------------------------------------------------------
col3, col4 = st.columns(2)

with col3:
    if not transfer_summary_df.empty:
        fig3 = go.Figure(
            data=[
                go.Pie(
                    labels=transfer_summary_df["Service"],
                    values=transfer_summary_df["Transfer Fees"],
                    hole=0.5,
                    marker_colors=["#008afa" if s == "Token Transfers" else "#ff8700" for s in transfer_summary_df["Service"]],
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>%{value} USD"
                )
            ]
        )
        fig3.update_layout(
            title="Total Transfer Fees By Service",
            height=500,
            legend=dict(orientation="v", x=1.05, y=0.5)
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("No data for fee distribution.")

with col4:
    if not transfer_summary_df.empty:
        fig4 = go.Figure(
            data=[
                go.Bar(
                    x=transfer_summary_df["Service"],
                    y=transfer_summary_df["Avg"],
                    marker_color=["#008afa" if s == "Token Transfers" else "#ff8700" for s in transfer_summary_df["Service"]],
                )
            ]
        )
        fig4.update_layout(
            title="Average Transfer Fees By Service",
            yaxis_title="$USD",
            height=500,
            xaxis_title="Service",
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.warning("No data for average fees.")

# -- Row 7, 8 ---------------------------------------------------------
# Define color mapping for directions
direction_colors = {
    "filecoin➡⛓": "#fa0610",
    "⛓➡filecoin": "#0090ff"
}

# Create 4 columns in a single row
col1, col2, col3, col4 = st.columns(4)

# --- Column 1: Donut - Volume ---
with col1:
    if not directional_df.empty:
        fig1 = go.Figure(
            data=[
                go.Pie(
                    labels=directional_df["Direction"],
                    values=directional_df["Transfer Volume"],
                    hole=0.5,
                    marker=dict(colors=[direction_colors[d] for d in directional_df["Direction"]]),
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>%{value} USD"
                )
            ]
        )
        fig1.update_layout(
            title="Volume of Transfers By Direction",
            height=500,
            legend=dict(orientation="v", x=1.05, y=0.5)
        )
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.warning("No data for volume by direction.")

# --- Column 2: Bar - Transfers & Users ---
with col2:
    if not directional_df.empty:
        fig2 = go.Figure(data=[
            go.Bar(
                x=directional_df["Direction"],
                y=directional_df["Transfer Count"],
                name="Transfer Count"
            ),
            go.Bar(
                x=directional_df["Direction"],
                y=directional_df["User Count"],
                name="User Count"
            )
        ])
        fig2.update_layout(
            barmode="group",
            title="Number of Transfers & Users",
            yaxis_title="Count",
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0)
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No data for transfer/user count.")

# --- Column 3: Donut - Fees ---
with col3:
    if not directional_df.empty:
        fig3 = go.Figure(
            data=[
                go.Pie(
                    labels=directional_df["Direction"],
                    values=directional_df["Transfer Fees"],
                    hole=0.5,
                    marker=dict(colors=[direction_colors[d] for d in directional_df["Direction"]]),
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>%{value} USD"
                )
            ]
        )
        fig3.update_layout(
            title="Total Transfer Fees",
            height=500,
            legend=dict(orientation="v", x=1.05, y=0.5)
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("No data for fees by direction.")

# --- Column 4: Bar - Avg Fee ---
with col4:
    if not directional_df.empty:
        fig4 = go.Figure(
            data=[
                go.Bar(
                    x=directional_df["Direction"],
                    y=directional_df["Avg"],
                    marker_color=[direction_colors[d] for d in directional_df["Direction"]]
                )
            ]
        )
        fig4.update_layout(
            title="Average Transfer Fees",
            yaxis_title="Avg Fee (USD)",
            height=500,
            xaxis_title="Direction",
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.warning("No data for average fee by direction.")
