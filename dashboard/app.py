"""
TradeFlow NG — Admin Dashboard v3
Fully native Streamlit components. No custom HTML cards.
Robust, clean, and maintainable.
"""

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
import sys, os


import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from auth import require_admin_login

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Read DATABASE_URL from Streamlit secrets or environment
try:
    db_url = st.secrets["database"]["DATABASE_URL"]
    os.environ["DATABASE_URL"] = db_url
except (KeyError, FileNotFoundError):
    pass  # Falls back to SQLite locally

st.set_page_config(
    page_title="TradeFlow NG",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

import plotly.io as pio

pio.templates["tradeflow"] = go.layout.Template(
    layout=go.Layout(
        font=dict(family="DM Sans, sans-serif", color="#1A1A1A", size=12),
        title=dict(font=dict(color="#1A6B3C", size=16, family="DM Sans")),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(
            tickfont=dict(color="#333333"),
            title_font=dict(color="#333333"),
            gridcolor="#EEEEEE",
        ),
        yaxis=dict(
            tickfont=dict(color="#333333"),
            title_font=dict(color="#333333"),
            gridcolor="#EEEEEE",
        ),
        legend=dict(font=dict(color="#333333")),
        colorway=["#1A6B3C","#E07B00","#1A5276","#8B5E3C","#C0392B"],
    )
)
pio.templates.default = "tradeflow"

DB_PATH = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"

# ── Colors (for Plotly only — not injected into CSS) ──────
GREEN  = "#1A6B3C"
AMBER  = "#E07B00"
RED    = "#C0392B"
BLUE   = "#1A5276"
GRAY   = "#555555"

# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Load DATABASE_URL from Streamlit secrets into environment
try:
    os.environ["DATABASE_URL"] = st.secrets["database"]["DATABASE_URL"]
except (KeyError, FileNotFoundError):
    pass

from db_adapter import query, execute, get_connection

@st.cache_resource

def naira(v):
    try: return f"₦{float(v):,.0f}"
    except: return "₦—"

def pct(v):
    try: return f"{float(v):.1f}%"
    except: return "—"

def safe_int(series_or_val):
    """Safely convert a Series or scalar to int."""
    try:
        if hasattr(series_or_val, 'iloc'):
            return int(pd.to_numeric(series_or_val, errors='coerce').fillna(0).sum())
        return int(float(series_or_val))
    except:
        return 0

def load_recs_overview():
    """Load recommendations for the latest run — overview version."""
    return query("""
        SELECT
            r.id,
            r.recommended_quantity,
            r.buy_price,
            r.sell_price,
            r.transport_cost,
            r.expected_profit_ngn,
            r.profit_margin_pct,
            COALESCE(r.is_shock_flagged, 0) AS is_shock_flagged,
            COALESCE(r.is_backhaul, 0)      AS is_backhaul,
            r.status,
            (r.sell_price - r.buy_price - r.transport_cost) AS profit_per_unit,
            co.name AS commodity_name
        FROM optimization_recommendations r
        JOIN commodities co ON r.commodity_id = co.id
        WHERE r.run_id = (SELECT MAX(id) FROM optimization_runs)
    """)

def load_recs_full(run_id):
    """Load full recommendations with corridor details."""
    return query("""
        SELECT
            r.id,
            r.recommended_quantity,
            r.buy_price,
            r.sell_price,
            r.transport_cost,
            r.expected_profit_ngn,
            r.profit_margin_pct,
            COALESCE(r.is_shock_flagged, 0) AS is_shock_flagged,
            COALESCE(r.is_backhaul, 0)      AS is_backhaul,
            r.status,
            (r.sell_price - r.buy_price - r.transport_cost) AS profit_per_unit,
            co.name      AS commodity,
            s_orig.name  AS origin,
            s_dest.name  AS destination,
            corr.distance_km,
            corr.road_quality,
            c2.perishability_class
        FROM optimization_recommendations r
        JOIN commodities co   ON r.commodity_id = co.id
        JOIN commodities c2   ON r.commodity_id = c2.id
        LEFT JOIN corridors corr ON r.corridor_id      = corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id = s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id   = s_dest.id
        WHERE r.run_id = ?
        ORDER BY r.expected_profit_ngn DESC
    """, (run_id,))


if not require_admin_login():
    st.stop()

# ══════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════

with st.sidebar:
    st.image("https://img.icons8.com/emoji/48/sheaf-of-rice.png", width=48)
    st.title("TradeFlow NG")
    st.caption("Internal Control Dashboard")
    st.divider()

    tab = st.radio("Navigate", [
        "📊 Overview",
        "🚚 Recommendations",
        "📋 Tableau",
        "📈 Forecasts",
        "📝 Feedback",
        "⚙️ Data Management",
    ], label_visibility="visible")

    st.divider()
    st.markdown(f"📅 **{date.today().strftime('%d %b %Y')}**")
    st.divider()

    try:
        last_fc  = query("SELECT MAX(generated_on) as d FROM forecasts").iloc[0]["d"]
        last_opt = query("SELECT MAX(run_date) as d FROM optimization_runs").iloc[0]["d"]
        n_prices = int(query("SELECT COUNT(*) as n FROM cleaned_prices").iloc[0]["n"])
        today_s  = str(date.today())

        st.markdown("**System Status**")
        fc_icon  = "🟢" if last_fc  == today_s else "🟡"
        opt_icon = "🟢" if last_opt == today_s else "🟡"
        st.markdown(f"{fc_icon} Forecasts: `{last_fc or 'Never'}`")
        st.markdown(f"{opt_icon} Optimizer: `{last_opt or 'Never'}`")
        st.markdown(f"🟢 Prices: `{n_prices:,}` records")

        if last_fc != today_s:
            st.warning("Forecasts not run today")
        if last_opt != today_s:
            st.warning("Optimizer not run today")
    except Exception as e:
        st.error(f"DB: {e}")


# ══════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════

if tab == "📊 Overview":
    st.title("📊 Overview")
    st.caption("Bird's-eye view of TradeFlow NG — profit potential, price movements, and system health.")

    latest_run = query("SELECT * FROM optimization_runs ORDER BY run_date DESC LIMIT 1")
    recs       = load_recs_overview()

    n_prices_week = int(query("""
        SELECT COUNT(DISTINCT state_id || commodity_id) AS n
        FROM cleaned_prices WHERE price_date >= DATE('now','-7 days')
    """).iloc[0]["n"])

    # ── KPIs ──────────────────────────────────────────────
    st.subheader("This Week's Key Numbers")
    st.info("These five numbers summarise what the system recommends and how active your network is right now.")

    total_profit = float(latest_run.iloc[0]["total_profit_ngn"]) if not latest_run.empty else 0
    avg_margin   = float(recs["profit_margin_pct"].mean()) if not recs.empty else 0
    n_recs       = len(recs)
    n_backhaul   = safe_int(recs["is_backhaul"]) if not recs.empty else 0
    n_risk       = safe_int(recs["is_shock_flagged"]) if not recs.empty else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("💰 Expected Profit",    naira(total_profit),
              help="Total profit if all recommendations are executed.")
    k2.metric("🚚 Routes",             n_recs,
              help="Number of routes selected by the optimizer.")
    k3.metric("↩ Backhaul Routes",     n_backhaul,
              help="Routes with profitable return loads.")
    k4.metric("📦 Active Markets (7d)", n_prices_week,
              help="Unique state-commodity pairs with recent price data.")
    k5.metric("📈 Avg Margin",          pct(avg_margin),
              help="Average profit as % of sell price.")

    st.divider()

    # ── Price trend + Profit by commodity ─────────────────
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.subheader("📉 Price Trends — Last 8 Weeks")
        st.caption("Lines that diverge between states = a profitable trade corridor. Buy where the line is low, sell where it's high.")

        comm_opts = query("SELECT DISTINCT name FROM commodities ORDER BY name")["name"].tolist()
        sel_comm  = st.selectbox("Select commodity", comm_opts, key="ov_comm")

        trend = query("""
            SELECT cp.price_date AS Date, s.name AS State,
                   cp.price_per_unit AS Price
            FROM cleaned_prices cp
            JOIN states s ON cp.state_id = s.id
            JOIN commodities c ON cp.commodity_id = c.id
            WHERE c.name = ? AND cp.price_date >= DATE('now','-56 days')
            ORDER BY cp.price_date
        """, (sel_comm,))

        if not trend.empty:
            fig = px.line(
                trend, x="Date", y="Price", color="State",
                title=f"{sel_comm} — Price per Unit (₦) by State",
                labels={"Price": "Price (₦/unit)"},
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            fig.update_layout(
                plot_bgcolor="white", paper_bgcolor="white",
                height=360, margin=dict(l=0, r=0, t=40, b=0),
                legend=dict(orientation="h", y=-0.25),
                hovermode="x unified",
                font=dict(color="#1A1A1A", size=12),
            )
            fig.update_xaxes(tickfont=dict(color="#1A1A1A"))
            fig.update_yaxes(tickfont=dict(color="#1A1A1A"))
            st.plotly_chart(fig, width='stretch')
        else:
            st.info(f"No price data for {sel_comm} yet. Only Yam, Maize, Rice, and Tomato have dummy data.")

    with col_r:
        st.subheader("💰 Profit by Commodity")
        st.caption("Which commodities generate the most expected profit this week? Taller bar = bigger opportunity.")

        if not recs.empty:
            cp = recs.groupby("commodity_name")["expected_profit_ngn"].sum().reset_index()
            cp.columns = ["Commodity", "Profit"]
            cp = cp.sort_values("Profit", ascending=False)

            fig2 = px.bar(
                cp, x="Commodity", y="Profit",
                color="Commodity",
                title="Expected Profit by Commodity",
                color_discrete_sequence=[GREEN, AMBER, BLUE, "#8B5E3C", RED],
                text_auto=True,
            )
            fig2.update_traces(texttemplate="₦%{y:,.0f}", textposition="outside")
            fig2.update_layout(
                plot_bgcolor="white", paper_bgcolor="white",
                showlegend=False, height=360,
                margin=dict(l=0, r=0, t=40, b=0),
                yaxis_title="Profit (₦)",
                font=dict(color="#1A1A1A", size=12),
            )
            fig2.update_xaxes(tickfont=dict(color="#1A1A1A"))
            fig2.update_yaxes(tickfont=dict(color="#1A1A1A"))
            st.plotly_chart(fig2, width='stretch')
        else:
            st.info("Run the optimizer to see profit breakdown.")

    # ── North-South gap ────────────────────────────────────
    st.divider()
    st.subheader("🗺️ North vs South Price Gap")
    st.info(
        "Compares average prices in northern supply states (Nasarawa, Niger, Abuja) "
        "vs southern demand states (Lagos, Oyo, Ogun). "
        "The bigger the gap between blue and green bars, the more profitable the north→south corridor."
    )

    gap = query("""
        SELECT c.name AS Commodity,
               AVG(CASE WHEN s.zone='North' THEN cp.price_per_unit END) AS North,
               AVG(CASE WHEN s.zone='South' THEN cp.price_per_unit END) AS South
        FROM cleaned_prices cp
        JOIN states s ON cp.state_id = s.id
        JOIN commodities c ON cp.commodity_id = c.id
        WHERE cp.price_date >= DATE('now','-7 days')
        GROUP BY c.name
        HAVING North IS NOT NULL AND South IS NOT NULL
    """)

    if not gap.empty:
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            y=gap["Commodity"], x=gap["North"],
            name="North — avg buy price", orientation="h", marker_color=BLUE,
        ))
        fig3.add_trace(go.Bar(
            y=gap["Commodity"], x=gap["South"],
            name="South — avg sell price", orientation="h", marker_color=GREEN,
        ))
        fig3.update_layout(
            barmode="group",
            title="Avg Buy Price (North) vs Avg Sell Price (South) — Last 7 Days",
            plot_bgcolor="black", paper_bgcolor="white",
            height=320, margin=dict(l=0, r=0, t=40, b=0),
            xaxis_title="Price (₦/unit)",
            legend=dict(orientation="h", y=-0.3),
            font=dict(color="#1A1A1A", size=12),
        )
        fig3.update_xaxes(tickfont=dict(color="#1A1A1A"))
        fig3.update_yaxes(tickfont=dict(color="#1A1A1A"))
        st.plotly_chart(fig3, width='stretch')
    else:
        st.info("Not enough zonal data for gap analysis yet.")

    # ── Pipeline log ───────────────────────────────────────
    st.divider()
    st.subheader("🔧 Recent Pipeline Activity")
    st.info("A log of every time data was cleaned, forecasts generated, or the optimizer ran. ✅ = success. ❌ = check error.")

    logs = query("""
        SELECT run_type AS Pipeline, status AS Status,
               records_in AS 'Records In', records_out AS 'Records Out',
               ROUND(duration_secs, 1) AS 'Duration (s)', run_at AS 'Timestamp'
        FROM pipeline_logs ORDER BY run_at DESC LIMIT 10
    """)

    if not logs.empty:
        logs["Status"] = logs["Status"].apply(
            lambda s: f"✅ {s}" if s == "Success" else f"❌ {s}" if s == "Failed" else f"⚠️ {s}"
        )
        st.dataframe(logs, width='stretch', hide_index=True)
    else:
        st.info("No pipeline runs recorded yet.")


# ══════════════════════════════════════════════════════════
# TAB 2 — RECOMMENDATIONS
# ══════════════════════════════════════════════════════════

elif tab == "🚚 Recommendations":
    st.title("🚚 Weekly Trade Recommendations")
    st.caption(
        "The system's recommended trades this week, ranked by profit. "
        "Each row = one trade: what to buy, where, where to sell, and expected profit."
    )

    runs = query("""
        SELECT id, run_date, week_start, week_end, solver_status, total_profit_ngn
        FROM optimization_runs ORDER BY run_date DESC LIMIT 10
    """)

    if runs.empty:
        st.warning("No optimization runs yet.")
        st.info("Go to ⚙️ Data Management → click **Run Optimization Pipeline**.")
        st.stop()

    run_labels = [
        f"Run {r['id']} — {r['run_date']}  |  Profit: {naira(r['total_profit_ngn'])}"
        for _, r in runs.iterrows()
    ]
    sel_idx    = st.selectbox("Select optimization run:", range(len(run_labels)),
                               format_func=lambda i: run_labels[i])
    sel_run_id = int(runs.iloc[sel_idx]["id"])

    recs = load_recs_full(sel_run_id)

    if recs.empty:
        st.info("No recommendations for this run.")
        st.stop()

    # Convert boolean cols safely
    recs["is_shock_flagged"] = pd.to_numeric(recs["is_shock_flagged"], errors="coerce").fillna(0).astype(int)
    recs["is_backhaul"]      = pd.to_numeric(recs["is_backhaul"],      errors="coerce").fillna(0).astype(int)
    recs["no_cost"]          = recs["transport_cost"].fillna(0) == 0

    # ── KPIs ──────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("💰 Total Expected Profit",  naira(recs["expected_profit_ngn"].sum()),
              help="Sum of all route profits if every recommendation is acted on.")
    k2.metric("🚚 Routes",                 len(recs))
    k3.metric("⚠️ High-Risk Routes",       int(recs["is_shock_flagged"].sum()),
              help="Routes where the price forecast is uncertain.")
    k4.metric("📈 Avg Margin",             pct(recs["profit_margin_pct"].mean()),
              help="Average profit as % of sell price.")

    st.info(
        "**How to read this table:** BUY the commodity at the stated price in the origin state. "
        "TRANSPORT it (cost per unit shown). SELL at the destination for the forecasted price. "
        "⚠️ NO COST = transport cost is ₦0 (profit is overstated). "
        "⚠️ RISK = uncertain price forecast."
    )

    # ── Filters ───────────────────────────────────────────
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        comm_filter = st.multiselect("Filter by commodity",
                                     recs["commodity"].unique().tolist())
    with fc2:
        risk_only  = st.checkbox("High-risk routes only")
    with fc3:
        back_only  = st.checkbox("Backhaul routes only")

    filtered = recs.copy()
    if comm_filter:
        filtered = filtered[filtered["commodity"].isin(comm_filter)]
    if risk_only:
        filtered = filtered[filtered["is_shock_flagged"] == 1]
    if back_only:
        filtered = filtered[filtered["is_backhaul"] == 1]

    st.markdown(f"**Showing {len(filtered)} of {len(recs)} recommendations**")
    st.divider()

    # ── Recommendation rows ────────────────────────────────
    for _, row in filtered.iterrows():
        is_risk  = row["is_shock_flagged"] == 1
        is_back  = row["is_backhaul"] == 1
        no_cost  = bool(row["no_cost"])
        profit   = float(row["expected_profit_ngn"])
        margin   = float(row["profit_margin_pct"])
        profit_u = float(row["profit_per_unit"])
        dist     = row.get("distance_km")
        road     = str(row.get("road_quality") or "")
        perish   = str(row.get("perishability_class") or "")

        # Route header
        flags = []
        if is_risk:  flags.append("⚠️ HIGH RISK")
        if no_cost:  flags.append("⚠️ NO COST DATA")
        if is_back:  flags.append("↩ BACKHAUL")
        if perish == "Perishable": flags.append("⚡ PERISHABLE")
        flag_str = "   ".join(flags)

        route_info = f"  |  🛣️ {dist:.0f} km · {road} road" if dist else ""

        with st.container(border=True):
            h1, h2 = st.columns([3, 1])
            with h1:
                st.markdown(f"### {row['commodity']}")
                st.markdown(f"📍 **{row['origin']}** → **{row['destination']}**{route_info}")
            with h2:
                if flags:
                    for f in flags:
                        st.warning(f)

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Quantity",       f"{row['recommended_quantity']:.0f} units")
            c2.metric("Buy at",         naira(row["buy_price"]),
                      help="Current market price at origin state.")
            c3.metric("Sell at",        naira(row["sell_price"]),
                      help="Forecasted price at destination state.")
            c4.metric("Transport/unit", naira(row["transport_cost"]),
                      help="Cost to move one unit along this corridor.")
            c5.metric("Profit/unit",    naira(profit_u),
                      delta=None if profit_u <= 0 else f"+{naira(profit_u)}",
                      help="Sell price minus buy price minus transport cost.")
            c6.metric("Total Profit",   naira(profit),
                      help=f"Profit/unit × quantity. Margin: {margin:.1f}%")

        st.write("")  # spacing between cards

    st.divider()

    # ── Export ────────────────────────────────────────────
    export_df = filtered[[
        "commodity", "origin", "destination",
        "recommended_quantity", "buy_price", "sell_price",
        "transport_cost", "profit_per_unit",
        "expected_profit_ngn", "profit_margin_pct"
    ]].copy()
    export_df.columns = [
        "Commodity", "Origin", "Destination", "Quantity",
        "Buy Price", "Sell Price", "Transport Cost",
        "Profit/Unit", "Total Profit", "Margin %"
    ]
    st.download_button(
        "⬇️ Export Recommendations (CSV)",
        data=export_df.to_csv(index=False),
        file_name=f"recommendations_{date.today()}.csv",
        mime="text/csv"
    )


# ══════════════════════════════════════════════════════════
# TAB 3 — TABLEAU
# ══════════════════════════════════════════════════════════

elif tab == "📋 Tableau":
    st.title("📋 Transportation Tableau")
    st.caption(
        "A profit map — shows expected profit per unit for every "
        "origin → destination corridor. Green = profitable. Red = loss-making."
    )
    st.info(
        "**How to read this:** Each cell = profit per unit for moving that commodity "
        "from the row (origin) to the column (destination). "
        "Darkest green = best corridor. Red = unprofitable."
    )

    comm_list = query("SELECT DISTINCT name FROM commodities ORDER BY name")["name"].tolist()
    sel       = st.selectbox("Select commodity:", comm_list)

    tableau = query("""
        SELECT s_orig.name AS Origin, s_dest.name AS Destination,
               cp.price_per_unit AS buy_price,
               f.predicted_price AS sell_price,
               COALESCE(tc.cost_per_unit, 0) AS transport_cost,
               (f.predicted_price - cp.price_per_unit
                - COALESCE(tc.cost_per_unit, 0)) AS profit_per_unit,
               CASE WHEN tc.cost_per_unit IS NULL THEN 1 ELSE 0 END AS missing_cost,
               COALESCE(f.is_shock_flagged, 0) AS is_shock_flagged
        FROM corridors corr
        JOIN states s_orig ON corr.origin_state_id = s_orig.id
        JOIN states s_dest ON corr.dest_state_id   = s_dest.id
        JOIN commodities c ON c.name = ?
        JOIN cleaned_prices cp
            ON  cp.state_id     = corr.origin_state_id
            AND cp.commodity_id = c.id
            AND cp.price_date   = (
                SELECT MAX(p2.price_date) FROM cleaned_prices p2
                WHERE p2.state_id = corr.origin_state_id
                  AND p2.commodity_id = c.id
            )
        JOIN forecasts f
            ON  f.state_id      = corr.dest_state_id
            AND f.commodity_id  = c.id
            AND f.forecast_date = DATE('now', '+1 day')
            AND f.generated_on  = DATE('now')
        LEFT JOIN transport_costs tc
            ON  tc.corridor_id  = corr.id
            AND tc.commodity_id = c.id
            AND (tc.expiry_date IS NULL OR tc.expiry_date >= DATE('now'))
        WHERE corr.is_active = 1
    """, (sel,))

    if tableau.empty:
        st.info(
            f"No tableau data for **{sel}**. "
            "Currently only Yam, Maize, Rice, and Tomato have forecast data. "
            "Select one of those, or run the Forecasting Pipeline first."
        )
    else:
        pivot = tableau.pivot_table(
            index="Origin", columns="Destination",
            values="profit_per_unit", aggfunc="mean"
        )

        fig = go.Figure(go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0.0, RED], [0.45, "#F7DC6F"],
                        [0.55, "#F7DC6F"], [1.0, GREEN]],
            zmid=0,
            text=[[f"₦{v:,.0f}" if not np.isnan(v) else "—"
                   for v in r] for r in pivot.values],
            texttemplate="%{text}",
            hovertemplate="<b>%{y} → %{x}</b><br>Profit/unit: ₦%{z:,.0f}<extra></extra>",
            colorbar=dict(title="Profit/Unit (₦)", tickprefix="₦"),
        ))
        fig.update_layout(
            title=f"{sel} — Profit per Unit (₦) | Row = Origin · Column = Destination",
            height=460, plot_bgcolor="white", paper_bgcolor="white",
            margin=dict(l=0, r=0, t=50, b=0),
            xaxis_title="Destination (where to sell)",
            yaxis_title="Origin (where to buy)",
            font=dict(color="#1A1A1A", size=12),
        )
        fig.update_xaxes(tickfont=dict(color="#1A1A1A"))
        fig.update_yaxes(tickfont=dict(color="#1A1A1A"))
        st.plotly_chart(fig, width='stretch')

        # Top 3
        best = tableau[tableau["profit_per_unit"] > 0].nlargest(3, "profit_per_unit")
        if not best.empty:
            st.subheader("🏆 Top 3 Routes")
            for i, (_, r) in enumerate(best.iterrows(), 1):
                note = " *(no real transport cost yet)*" if r["missing_cost"] else ""
                st.markdown(
                    f"**{i}. {r['Origin']} → {r['Destination']}** — "
                    f"Profit: **{naira(r['profit_per_unit'])}/unit** "
                    f"(Buy: {naira(r['buy_price'])}, "
                    f"Sell forecast: {naira(r['sell_price'])}, "
                    f"Transport: {naira(r['transport_cost'])}){note}"
                )

        with st.expander("📋 Full breakdown table"):
            d = tableau.copy()
            for col in ["buy_price", "sell_price", "transport_cost", "profit_per_unit"]:
                d[col] = d[col].apply(naira)
            d["missing_cost"]     = d["missing_cost"].apply(lambda x: "⚠ Estimated" if x else "✓ Real")
            d["is_shock_flagged"] = d["is_shock_flagged"].apply(lambda x: "⚠ Uncertain" if x else "✓ Stable")
            st.dataframe(
                d.rename(columns={
                    "buy_price": "Buy Price",
                    "sell_price": "Sell Price (Forecast)",
                    "transport_cost": "Transport Cost",
                    "profit_per_unit": "Profit/Unit",
                    "missing_cost": "Cost Data",
                    "is_shock_flagged": "Forecast Quality",
                })[[
                    "Origin", "Destination", "Buy Price",
                    "Sell Price (Forecast)", "Transport Cost",
                    "Profit/Unit", "Cost Data", "Forecast Quality"
                ]],
                width='stretch', hide_index=True
            )


# ══════════════════════════════════════════════════════════
# TAB 4 — FORECASTS
# ══════════════════════════════════════════════════════════

elif tab == "📈 Forecasts":
    st.title("📈 Price Forecasts")
    st.caption("Prophet's 7-day price predictions. Use this to understand where prices are heading before committing to a trade.")
    st.info(
        "**How to read this chart:** "
        "Solid green line = actual historical prices. "
        "Dashed orange line = what Prophet predicts next week. "
        "Shaded area = uncertainty range (80% confidence — narrow = confident, wide = uncertain). "
        "Red ✕ markers = high-risk days where the model is especially uncertain."
    )

    c1, c2 = st.columns(2)
    with c1:
        sel_comm = st.selectbox(
            "Commodity",
            query("SELECT DISTINCT name FROM commodities ORDER BY name")["name"].tolist()
        )
    with c2:
        sel_state = st.selectbox(
            "State",
            query("SELECT DISTINCT name FROM states ORDER BY name")["name"].tolist()
        )

    fc = query("""
        SELECT f.forecast_date, f.predicted_price, f.lower_bound, f.upper_bound,
               COALESCE(f.is_shock_flagged, 0) AS is_shock_flagged, f.shock_reason
        FROM forecasts f
        JOIN states s      ON f.state_id     = s.id
        JOIN commodities c ON f.commodity_id = c.id
        WHERE s.name = ? AND c.name = ? AND f.generated_on = DATE('now')
        ORDER BY f.forecast_date
    """, (sel_state, sel_comm))

    hist = query("""
        SELECT cp.price_date AS Date, cp.price_per_unit AS Price
        FROM cleaned_prices cp
        JOIN states s      ON cp.state_id     = s.id
        JOIN commodities c ON cp.commodity_id = c.id
        WHERE s.name = ? AND c.name = ?
          AND cp.price_date >= DATE('now', '-56 days')
        ORDER BY cp.price_date
    """, (sel_state, sel_comm))

    if fc.empty and hist.empty:
        st.info(
            f"No data for **{sel_comm}** in **{sel_state}**. "
            "Currently only Yam, Maize, Rice, and Tomato have data. "
            "Try one of those, or run the Forecasting Pipeline."
        )
    else:
        fig = go.Figure()

        if not hist.empty:
            fig.add_trace(go.Scatter(
                x=hist["Date"], y=hist["Price"],
                mode="lines+markers", name="Historical Price",
                line=dict(color=GREEN, width=2), marker=dict(size=4),
                hovertemplate="Date: %{x}<br>Actual: ₦%{y:,.0f}<extra></extra>"
            ))

        if not fc.empty:
            fig.add_trace(go.Scatter(
                x=pd.concat([fc["forecast_date"], fc["forecast_date"][::-1]]),
                y=pd.concat([fc["upper_bound"],    fc["lower_bound"][::-1]]),
                fill="toself", fillcolor="rgba(26,107,60,0.10)",
                line=dict(color="rgba(0,0,0,0)"),
                name="80% Confidence Band", hoverinfo="skip",
            ))
            fig.add_trace(go.Scatter(
                x=fc["forecast_date"], y=fc["predicted_price"],
                mode="lines+markers", name="Forecast",
                line=dict(color=AMBER, width=2.5, dash="dash"),
                marker=dict(
                    size=10,
                    color=[RED if r else AMBER for r in fc["is_shock_flagged"]],
                    symbol=["x" if r else "circle" for r in fc["is_shock_flagged"]],
                    line=dict(width=2, color="white")
                ),
                hovertemplate="Date: %{x}<br>Forecast: ₦%{y:,.0f}<extra></extra>"
            ))

        # Today line
        today_ts = pd.Timestamp(date.today()).timestamp() * 1000
        fig.add_vline(x=today_ts, line_dash="dot", line_color=GRAY,
                      annotation_text="Today")

        fig.update_layout(
            title=f"{sel_comm} Price Forecast — {sel_state}",
            xaxis_title="Date", yaxis_title="Price (₦/unit)",
            plot_bgcolor="white", paper_bgcolor="white",
            height=440, margin=dict(l=0, r=0, t=50, b=0),
            legend=dict(orientation="h", y=-0.2),
            hovermode="x unified",
            font=dict(color="#1A1A1A", size=12),
        )
        fig.update_xaxes(tickfont=dict(color="#1A1A1A"))
        fig.update_yaxes(tickfont=dict(color="#1A1A1A"))
        st.plotly_chart(fig, width='stretch')

        if not fc.empty:
            cs1, cs2, cs3 = st.columns(3)
            cs1.metric("Next Week Avg Price",
                       naira(fc["predicted_price"].mean()),
                       help="Average predicted price across the 7 forecast days.")
            cs2.metric("Price Range",
                       f"{naira(fc['lower_bound'].min())} – {naira(fc['upper_bound'].max())}",
                       help="Full uncertainty band across all 7 days.")
            cs3.metric("High-Risk Days",
                       int(pd.to_numeric(fc["is_shock_flagged"], errors="coerce").fillna(0).sum()),
                       help="Days where forecast uncertainty is unusually high.")

            shocks = fc[pd.to_numeric(fc["is_shock_flagged"], errors="coerce").fillna(0) == 1]
            if not shocks.empty:
                st.warning(f"⚠️ {len(shocks)} high-risk day(s) detected. Included in recommendations but marked uncertain.")
                with st.expander("Why are these days flagged?"):
                    st.dataframe(
                        shocks[["forecast_date", "predicted_price",
                                "lower_bound", "upper_bound", "shock_reason"]],
                        width='stretch', hide_index=True
                    )

            with st.expander("📋 Full 7-day forecast table"):
                fd = fc.copy()
                for c in ["predicted_price", "lower_bound", "upper_bound"]:
                    fd[c] = fd[c].apply(naira)
                fd["is_shock_flagged"] = fd["is_shock_flagged"].apply(
                    lambda x: "⚠ High Risk" if int(x) == 1 else "✓ Normal"
                )
                st.dataframe(
                    fd[["forecast_date", "predicted_price", "lower_bound",
                        "upper_bound", "is_shock_flagged"]].rename(columns={
                        "forecast_date": "Date",
                        "predicted_price": "Predicted Price",
                        "lower_bound": "Lower Bound",
                        "upper_bound": "Upper Bound",
                        "is_shock_flagged": "Risk Level",
                    }),
                    width='stretch', hide_index=True
                )


# ══════════════════════════════════════════════════════════
# TAB 5 — FEEDBACK
# ══════════════════════════════════════════════════════════

elif tab == "📝 Feedback":
    st.title("📝 Log Actual Trade Outcomes")
    st.caption("Record what actually happened on a completed trade. This teaches the system and improves next week's predictions.")
    st.info(
        "**Why this matters:** The system recommends trades based on predicted prices. "
        "When you enter what actually happened — the real prices and costs — "
        "the system uses that to make better forecasts next time. "
        "The more outcomes you log, the smarter the system becomes."
    )

    st.subheader("How to log a completed trade")
    st.markdown("""
1. **Select** the recommendation that was acted on from the dropdown below
2. **Check** what the system predicted vs what you'll enter as actual figures
3. **Enter** the real buy price, sell price, transport cost, and quantity moved
4. **Add notes** about market conditions, delays, or surprises (optional)
5. **Click Submit** — the outcome is saved and used to improve future forecasts
""")

    pending = query("""
        SELECT r.id, co.name AS commodity,
               s_orig.name AS origin, s_dest.name AS destination,
               r.recommended_quantity, r.buy_price,
               r.sell_price, r.expected_profit_ngn, r.status
        FROM optimization_recommendations r
        JOIN commodities co ON r.commodity_id = co.id
        LEFT JOIN corridors corr ON r.corridor_id      = corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id = s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id   = s_dest.id
        WHERE r.status = 'Pending'
        ORDER BY r.id DESC LIMIT 30
    """)

    if pending.empty:
        st.info("No pending recommendations to log. All have been completed or no optimization has run yet.")
    else:
        rec_labels = [
            f"#{r['id']} — {r['commodity']} | {r['origin']} → {r['destination']} "
            f"(predicted: {naira(r['expected_profit_ngn'])})"
            for _, r in pending.iterrows()
        ]
        sel_idx = st.selectbox("Which trade are you reporting on?",
                               range(len(rec_labels)),
                               format_func=lambda i: rec_labels[i])
        sel = pending.iloc[sel_idx]

        st.subheader("What the system predicted:")
        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("Predicted buy price",  naira(sel["buy_price"]))
        pc2.metric("Predicted sell price", naira(sel["sell_price"]))
        pc3.metric("Recommended quantity", f"{sel['recommended_quantity']:.0f} units")
        pc4.metric("Predicted profit",     naira(sel["expected_profit_ngn"]))

        st.subheader("Enter what actually happened:")

        with st.form("feedback_form"):
            fc1, fc2 = st.columns(2)
            with fc1:
                actual_buy       = st.number_input("Actual buy price (₦/unit)",
                                                   value=float(sel["buy_price"] or 0), step=100.0,
                                                   help="Price you actually paid per unit at the origin market.")
                actual_sell      = st.number_input("Actual sell price (₦/unit)",
                                                   value=float(sel["sell_price"] or 0), step=100.0,
                                                   help="Price you actually received per unit at destination.")
                actual_qty       = st.number_input("Actual quantity moved (units)",
                                                   value=float(sel["recommended_quantity"] or 0), step=1.0)
            with fc2:
                actual_transport = st.number_input("Actual transport cost (₦/unit)",
                                                   value=0.0, step=100.0,
                                                   help="Total transport cost ÷ units moved.")
                trip_date        = st.date_input("Date of trade", value=date.today())
                notes            = st.text_area("Notes (optional)",
                                                placeholder="e.g. Market was flooded, prices were higher than usual...")

            if actual_sell > 0:
                preview = actual_qty * (actual_sell - actual_buy - actual_transport)
                diff    = preview - float(sel["expected_profit_ngn"] or 0)
                diff_str = f"▲ {naira(diff)} more than predicted" if diff >= 0 \
                           else f"▼ {naira(abs(diff))} less than predicted"
                st.info(f"📊 **Preview:** Actual profit = **{naira(preview)}** ({diff_str})")

            if st.form_submit_button("✅ Submit Outcome", type="primary"):
                actual_profit = actual_qty * (actual_sell - actual_buy - actual_transport)
                conn = get_connection()
                conn.execute("""
                    INSERT INTO actual_outcomes (
                        recommendation_id, commodity_id, corridor_id,
                        actual_buy_price, actual_sell_price,
                        actual_transport_cost, actual_quantity,
                        actual_profit_ngn, trip_date, outcome_notes, data_source
                    )
                    SELECT ?, r.commodity_id, r.corridor_id,
                           ?, ?, ?, ?, ?, ?, ?, 'Dashboard'
                    FROM optimization_recommendations r WHERE r.id = ?
                """, (int(sel["id"]), actual_buy, actual_sell, actual_transport,
                      actual_qty, round(actual_profit, 2), str(trip_date),
                      notes, int(sel["id"])))
                conn.execute(
                    "UPDATE optimization_recommendations SET status='Completed' WHERE id=?",
                    (int(sel["id"]),)
                )
                conn.commit()
                st.success(f"✅ Outcome saved! Actual profit: **{naira(actual_profit)}**")
                st.balloons()

    st.divider()
    st.subheader("📚 Previously Logged Outcomes")
    st.caption("All outcomes logged so far. This data will feed Prophet retraining.")

    outcomes = query("""
        SELECT ao.trip_date AS Date, c.name AS Commodity,
               ao.actual_buy_price AS 'Buy Price',
               ao.actual_sell_price AS 'Sell Price',
               ao.actual_transport_cost AS 'Transport Cost',
               ao.actual_quantity AS Quantity,
               ao.actual_profit_ngn AS 'Actual Profit',
               ao.outcome_notes AS Notes
        FROM actual_outcomes ao
        JOIN commodities c ON ao.commodity_id = c.id
        ORDER BY ao.trip_date DESC LIMIT 20
    """)

    if not outcomes.empty:
        for col in ["Buy Price", "Sell Price", "Transport Cost", "Actual Profit"]:
            outcomes[col] = outcomes[col].apply(naira)
        st.dataframe(outcomes, width='stretch', hide_index=True)
    else:
        st.info("No outcomes logged yet.")


# ══════════════════════════════════════════════════════════
# TAB 6 — DATA MANAGEMENT
# ══════════════════════════════════════════════════════════

elif tab == "⚙️ Data Management":
    st.title("⚙️ Data Management")
    st.caption("The engine room. Run pipelines, upload agent data, and inspect the database.")
    st.info(
        "**Run these three pipelines every week in order:** "
        "Step 1 → Clean → Step 2 → Forecast → Step 3 → Optimize. "
        "Each step feeds into the next."
    )

    # ── Pipelines ─────────────────────────────────────────
    st.subheader("🔄 Run Pipelines (in order)")

    with st.container(border=True):
        st.markdown("**Step 1 — 🧹 Cleaning Pipeline**")
        st.caption(
            "Reads raw agent submissions, removes bad values, and saves clean "
            "price records to the database. Run whenever new Kobo or CSV data arrives."
        )
        if st.button("▶ Run Cleaning Pipeline", key="run_clean"):
            with st.spinner("Cleaning data..."):
                try:
                    from cleaning import run_cleaning_pipeline
                    run_cleaning_pipeline(source="raw")
                    st.success("✅ Cleaning complete.")
                except Exception as e:
                    st.error(f"❌ {e}")

    with st.container(border=True):
        st.markdown("**Step 2 — 📈 Forecasting Pipeline**")
        st.caption(
            "Trains Prophet on cleaned prices and predicts next week's prices "
            "for every commodity in every state. Takes 2–3 minutes."
        )
        if st.button("▶ Run Forecasting Pipeline", key="run_fc"):
            with st.spinner("Training forecast models — please wait (2–3 mins)..."):
                try:
                    from forecasting import run_forecasting_pipeline
                    run_forecasting_pipeline(periods=7)
                    st.success("✅ Forecasting complete.")
                except Exception as e:
                    st.error(f"❌ {e}")

    with st.container(border=True):
        st.markdown("**Step 3 — 🚚 Optimization Pipeline**")
        st.caption(
            "Uses forecasted prices to find the most profitable trade routes. "
            "Results appear in the Recommendations tab."
        )
        if st.button("▶ Run Optimization Pipeline", key="run_opt"):
            with st.spinner("Finding best routes..."):
                try:
                    from optimization import run_optimization_pipeline
                    run_optimization_pipeline()
                    st.success("✅ Optimization complete. View results in 🚚 Recommendations.")
                except Exception as e:
                    st.error(f"❌ {e}")

    if st.button("📊 Check Forecast Accuracy", key="check_acc"):
        with st.spinner("Evaluating..."):
            try:
                from scheduler import compute_forecast_accuracy, MAPE_THRESHOLD
                acc = compute_forecast_accuracy()
                if acc.empty:
                    st.info("Not enough outcome data yet. Log at least 5 completed trades first.")
                else:
                    st.dataframe(acc, width='stretch', hide_index=True)
                    needs = acc[acc["needs_retrain"]]
                    if not needs.empty:
                        st.warning(f"⚠️ {len(needs)} commodity/ies exceed {MAPE_THRESHOLD}% error threshold and need retraining.")
                    else:
                        st.success(f"✅ All commodities within {MAPE_THRESHOLD}% accuracy threshold.")
            except Exception as e:
                st.error(f"❌ {e}")

    st.divider()

    st.subheader("👤 Register New Agent")
    with st.form("register_agent"):
        col1, col2 = st.columns(2)
        with col1:
            a_name  = st.text_input("Full name")
            a_phone = st.text_input("Phone number", placeholder="08012345678")
        with col2:
            states  = query("SELECT id, name FROM states ORDER BY name")
            a_state = st.selectbox("State", states["name"].tolist())
            a_role  = st.selectbox("Role", ["Reporter", "Supervisor"])

        if st.form_submit_button("Register Agent", type="primary"):
            state_id = int(states[states["name"] == a_state].iloc[0]["id"])
            try:
                execute("""
                    INSERT INTO agents (full_name, phone, state_id, role, is_active)
                    VALUES (?, ?, ?, ?, 1)
                """, (a_name, a_phone, state_id, a_role))
                st.success(f"✅ {a_name} registered. They can now log in with {a_phone}.")
            except Exception as e:
                st.error(f"❌ {e} — phone number may already exist.")

    # ── CSV Upload ─────────────────────────────────────────
    st.subheader("📂 Upload Agent Price Reports")
    st.info(
        "Agents submit price data via CSV or Excel. "
        "Upload the file here to add it to the database. "
        "After uploading, run the Cleaning Pipeline (Step 1 above) to process it."
    )

    cu1, cu2 = st.columns([3, 1])
    with cu1:
        uploaded = st.file_uploader("Upload CSV or Excel file", type=["csv", "xlsx"])
        if uploaded:
            import tempfile
            suffix = ".csv" if uploaded.name.endswith(".csv") else ".xlsx"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded.read())
                tmp_path = tmp.name
            try:
                from csv_uploader import ingest_csv
                ok = ingest_csv(tmp_path)
                if ok:
                    st.success("✅ File uploaded. Now run the Cleaning Pipeline above.")
                else:
                    st.error("❌ File format issue. Check columns match the template.")
            except Exception as e:
                st.error(f"❌ {e}")

    with cu2:
        st.markdown("**Need the template?**")
        st.caption("Download and send to agents.")
        try:
            from csv_uploader import generate_template
            import tempfile
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            generate_template(tmp.name)
            with open(tmp.name) as f:
                tpl = f.read()
            st.download_button("⬇️ Template CSV", data=tpl,
                               file_name="agent_report_template.csv", mime="text/csv")
        except Exception as e:
            st.error(f"❌ {e}")

    st.divider()

    # ── DB Inspector ───────────────────────────────────────
    st.subheader("🗄️ Database Inspector")
    st.info(
        "Shows every table and how many records it contains. "
        "Use the preview to check data quality or debug issues."
    )

    tables = [
        "states", "markets", "commodities", "agents", "corridors",
        "raw_submissions", "cleaned_prices", "transport_costs",
        "forecasts", "optimization_runs",
        "optimization_recommendations", "actual_outcomes", "pipeline_logs"
    ]

    counts = {}
    for t in tables:
        try:
            counts[t] = int(query(f"SELECT COUNT(*) AS n FROM {t}").iloc[0]["n"])
        except:
            counts[t] = "error"

    cdf = pd.DataFrame([{
        "Table":   t,
        "Records": counts[t],
        "Status":  ("✅ Has data" if isinstance(counts[t], int) and counts[t] > 0
                    else "⚪ Empty"  if counts[t] == 0
                    else "❌ Error"),
    } for t in tables])
    st.dataframe(cdf, width='stretch', hide_index=True)

    st.markdown("**Preview a table:**")
    sel_table = st.selectbox("Select table to preview", tables)
    n_rows    = st.slider("Number of rows", 5, 50, 10)

    try:
        preview = query(f"SELECT * FROM {sel_table} LIMIT {n_rows}")
        st.dataframe(preview, width='stretch', hide_index=True)
        st.caption(
            f"Showing {len(preview)} of {counts[sel_table]} "
            f"total records in `{sel_table}`."
        )
    except Exception as e:
        st.error(f"Could not load: {e}")
