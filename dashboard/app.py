"""
TradeFlow NG — Admin Dashboard
Fixed: boolean errors, conn.execute, dangling decorator,
       duplicate imports, SQLite date functions in cloud,
       agent registration with agent_id + password.
"""

# ── DATABASE_URL must be first — before db_adapter import ─
import os, sys
import streamlit as st

try:
    os.environ["DATABASE_URL"] = st.secrets["database"]["DATABASE_URL"]
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from db_adapter import query, execute, get_connection, backend_name

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from datetime import date, timedelta

# ── Logo paths ─────────────────────────────────────────────
_ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')
LOGO_FULL  = os.path.join(_ASSET_DIR, 'logo-full.png')
LOGO_ICON  = os.path.join(_ASSET_DIR, 'logo-icon.png')
if not os.path.exists(LOGO_FULL):
    LOGO_FULL = os.path.join(_ASSET_DIR, 'TradeFlow profile.png')
if not os.path.exists(LOGO_ICON):
    LOGO_ICON = os.path.join(_ASSET_DIR, 'TradeFlow logo.png')
_icon = LOGO_ICON if os.path.exists(LOGO_ICON) else "🌾"

# ── Page config ────────────────────────────────────────────
st.set_page_config(
    page_title="TradeFlow NG — Admin",
    page_icon=_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state init before auth ─────────────────────────
for _k, _v in [("admin_authenticated", False), ("admin_user", None)]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

from auth import require_admin_login

# ── Plotly template ────────────────────────────────────────
GREEN = "#1A6B3C"; AMBER = "#E07B00"; RED = "#C0392B"
BLUE  = "#1A5276"; GRAY  = "#555555"; LIME = "#2ECC71"

pio.templates["tfng"] = go.layout.Template(
    layout=go.Layout(
        font=dict(family="Plus Jakarta Sans, sans-serif",
                  color="#1A1A1A", size=12),
        title=dict(font=dict(color=GREEN, size=15)),
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(gridcolor="#EEEEEE", tickfont=dict(color="#333")),
        yaxis=dict(gridcolor="#EEEEEE", tickfont=dict(color="#333")),
        colorway=[GREEN, AMBER, BLUE, "#8B5E3C", RED],
    )
)
pio.templates.default = "tfng"

# ══════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0D1F14 !important;
    border-right: 1px solid rgba(255,255,255,0.06);
}
[data-testid="stSidebar"] * { color: rgba(255,255,255,0.85) !important; }
[data-testid="stSidebar"] .stRadio label {
    border-radius: 10px;
    padding: 10px 14px !important;
    margin-bottom: 3px;
    transition: background 0.2s;
    font-weight: 500 !important;
    font-size: 0.9rem !important;
    color: rgba(255,255,255,0.7) !important;
}
[data-testid="stSidebar"] .stRadio label:hover {
    background: rgba(255,255,255,0.08) !important;
    color: white !important;
}

/* Main area */
.main .block-container { padding-top: 2rem; padding-bottom: 3rem; }

/* Metrics */
[data-testid="metric-container"] {
    background: white;
    border-radius: 14px;
    border: 1px solid rgba(26,107,60,0.12);
    border-top: 3px solid #1A6B3C;
    padding: 16px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.06);
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    color: #6B7A70 !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 1.5rem !important;
    font-weight: 800 !important;
    color: #0D1F14 !important;
}

/* Containers / cards */
[data-testid="stContainer"] {
    background: white;
    border-radius: 16px;
    border: 1px solid rgba(26,107,60,0.12);
    box-shadow: 0 2px 10px rgba(0,0,0,0.05);
}

/* Buttons */
.stButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}
.stButton > button[kind="primary"] {
    background: #1A6B3C !important;
    color: white !important;
    border: none !important;
}
.stButton > button[kind="primary"]:hover {
    background: #2E8B57 !important;
    transform: translateY(-1px);
    box-shadow: 0 4px 14px rgba(26,107,60,0.3) !important;
}

/* DataFrames */
.stDataFrame { border-radius: 12px; overflow: hidden; }

/* Titles */
h1 { font-weight: 800 !important; color: #0D1F14 !important; letter-spacing: -0.02em !important; }
h2 { font-weight: 700 !important; color: #1A1A1A !important; }
h3 { font-weight: 600 !important; color: #1A6B3C !important; }

/* Forms */
.stForm { border-radius: 14px !important; }

/* Divider */
hr { border-color: rgba(26,107,60,0.1) !important; }

/* Main content area - force white background so headers readable */
.main { background: #F5F5F5 !important; }
.main .block-container { background: #F5F5F5 !important; }

/* Page headers - force dark on light background */
h1 { color: #0D1F14 !important; font-weight: 800 !important; letter-spacing:-0.02em !important; }
h2 { color: #1A1A1A !important; font-weight: 700 !important; }
h3 { color: #1A6B3C !important; font-weight: 600 !important; }
p, li, .stMarkdown { color: #1A1A1A !important; }
.stCaption, [data-testid="stCaptionContainer"] * { color: #6B7A70 !important; }

/* Form labels - dark text on white form backgrounds */
.stForm label,
.stTextInput label, .stNumberInput label,
.stSelectbox label, .stTextArea label,
.stDateInput label, .stFileUploader label {
    color: #1A1A1A !important;
    font-weight: 600 !important;
}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════

def naira(v):
    try:    return f"₦{float(v):,.0f}"
    except: return "₦—"

def pct(v):
    try:    return f"{float(v):.1f}%"
    except: return "—"

def safe_int(series_or_val):
    try:
        if hasattr(series_or_val, 'iloc'):
            return int(pd.to_numeric(series_or_val, errors='coerce').fillna(0).sum())
        return int(float(series_or_val))
    except:
        return 0

def safe_bool_sum(series):
    """Sum a boolean/int column safely."""
    try:
        return int(pd.to_numeric(series, errors='coerce').fillna(0).sum())
    except:
        return 0


def load_recs_overview():
    return query("""
        SELECT r.id,
               r.recommended_quantity,
               r.buy_price, r.sell_price, r.transport_cost,
               r.expected_profit_ngn, r.profit_margin_pct,
               COALESCE(r.is_shock_flagged, FALSE) AS is_shock_flagged,
               COALESCE(r.is_backhaul,      FALSE) AS is_backhaul,
               r.status,
               (r.sell_price - r.buy_price - r.transport_cost) AS profit_per_unit,
               co.name AS commodity_name
        FROM   optimization_recommendations r
        JOIN   commodities co ON r.commodity_id = co.id
        WHERE  r.run_id = (SELECT MAX(id) FROM optimization_runs)
    """)


def load_recs_full(run_id):
    return query("""
        SELECT r.id,
               r.recommended_quantity,
               r.buy_price, r.sell_price, r.transport_cost,
               r.expected_profit_ngn, r.profit_margin_pct,
               COALESCE(r.is_shock_flagged, FALSE) AS is_shock_flagged,
               COALESCE(r.is_backhaul,      FALSE) AS is_backhaul,
               r.status,
               (r.sell_price - r.buy_price - r.transport_cost) AS profit_per_unit,
               co.name      AS commodity,
               s_orig.name  AS origin,
               s_dest.name  AS destination,
               corr.distance_km, corr.road_quality,
               c2.perishability_class
        FROM   optimization_recommendations r
        JOIN   commodities co   ON r.commodity_id       = co.id
        JOIN   commodities c2   ON r.commodity_id       = c2.id
        LEFT JOIN corridors corr ON r.corridor_id       = corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id = s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id   = s_dest.id
        WHERE  r.run_id = ?
        ORDER BY r.expected_profit_ngn DESC
    """, (run_id,))


# ══════════════════════════════════════════════════════════
# AUTH GATE
# ══════════════════════════════════════════════════════════

if not require_admin_login():
    st.stop()

# ══════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════

with st.sidebar:
    if os.path.exists(LOGO_FULL):
        st.image(LOGO_FULL, width=190)
    else:
        st.markdown("## 🌾 TradeFlow NG")

    st.caption("Internal Control Dashboard")
    st.divider()

    tab = st.radio("Navigation", [
        "📊 Overview",
        "🚚 Recommendations",
        "📋 Tableau",
        "📈 Forecasts",
        "📝 Feedback",
        "⚙️ Data Management",
    ], label_visibility="collapsed")

    st.divider()
    st.markdown(f"📅 **{date.today().strftime('%d %b %Y')}**")
    st.caption(f"DB: `{backend_name()}`")
    st.divider()

    try:
        last_fc  = query("SELECT MAX(generated_on) AS d FROM forecasts").iloc[0]["d"]
        last_opt = query("SELECT MAX(run_date) AS d FROM optimization_runs").iloc[0]["d"]
        n_prices = int(query("SELECT COUNT(*) AS n FROM cleaned_prices").iloc[0]["n"])
        today_s  = str(date.today())

        # Convert to string safely — PostgreSQL returns datetime objects
        last_fc_s  = str(last_fc)[:10]  if last_fc  else None
        last_opt_s = str(last_opt)[:10] if last_opt else None

        fc_ok  = last_fc_s  == today_s
        opt_ok = last_opt_s == today_s

        st.markdown("**System Status**")
        st.markdown(f"{'🟢' if fc_ok  else '🟡'} Forecasts: `{last_fc_s  or 'Never'}`")
        st.markdown(f"{'🟢' if opt_ok else '🟡'} Optimizer: `{last_opt_s or 'Never'}`")
        st.markdown(f"🟢 Price records: `{n_prices:,}`")
        if not fc_ok:
            st.warning("Forecasts not run today")
        if not opt_ok:
            st.warning("Optimizer not run today")
    except Exception as e:
        st.error(f"Status error: {e}")


# ══════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════

if tab == "📊 Overview":
    st.title("📊 Overview")
    st.caption("Bird's-eye view of TradeFlow NG — profit potential, price movements, and system health.")

    latest_run = query("SELECT * FROM optimization_runs ORDER BY run_date DESC LIMIT 1")
    recs       = load_recs_overview()

    # ── Price coverage — uses db_adapter so DATE translation applies ──
    n_prices_week = safe_int(query("""
        SELECT COUNT(DISTINCT CAST(state_id AS TEXT) || CAST(commodity_id AS TEXT)) AS n
        FROM   cleaned_prices
        WHERE  price_date >= (CURRENT_DATE - INTERVAL '7 days')
          AND  is_outlier  IS NOT TRUE
    """).iloc[0]["n"])

    total_profit = float(latest_run.iloc[0]["total_profit_ngn"]) \
                   if not latest_run.empty else 0
    avg_margin   = float(recs["profit_margin_pct"].mean()) if not recs.empty else 0
    n_recs       = len(recs)
    n_backhaul   = safe_bool_sum(recs["is_backhaul"])   if not recs.empty else 0
    n_risk       = safe_bool_sum(recs["is_shock_flagged"]) if not recs.empty else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("💰 Expected Profit",     naira(total_profit))
    k2.metric("🚚 Routes",              n_recs)
    k3.metric("↩ Backhaul",            n_backhaul)
    k4.metric("📦 Active Pairs (7d)",  n_prices_week)
    k5.metric("📈 Avg Margin",          pct(avg_margin))

    st.divider()
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.subheader("📉 Price Trends — Last 8 Weeks")
        comm_opts = query("SELECT DISTINCT name FROM commodities ORDER BY name")["name"].tolist()
        sel_comm  = st.selectbox("Commodity", comm_opts, key="ov_comm")

        # SQLite DATE translated by db_adapter
        trend = query("""
            SELECT cp.price_date AS date,
                   s.name        AS state,
                   cp.price_per_unit AS price
            FROM   cleaned_prices cp
            JOIN   states      s ON cp.state_id     = s.id
            JOIN   commodities c ON cp.commodity_id = c.id
            WHERE  c.name       = ?
              AND  cp.price_date >= DATE('now','-56 days')
              AND  cp.is_outlier IS NOT TRUE
            ORDER BY cp.price_date
        """, (sel_comm,))

        if not trend.empty:
            fig = px.line(
                trend, x="date", y="price", color="state",
                title=f"{sel_comm} — Price per Unit (₦) by State",
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            fig.update_layout(height=360, margin=dict(l=0, r=0, t=40, b=0),
                              legend=dict(orientation="h", y=-0.25),
                              hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No price data for {sel_comm} yet.")

    with col_r:
        st.subheader("💰 Profit by Commodity")
        if not recs.empty:
            cp = recs.groupby("commodity_name")["expected_profit_ngn"].sum().reset_index()
            cp.columns = ["Commodity", "Profit"]
            cp = cp.sort_values("Profit", ascending=False)
            fig2 = px.bar(cp, x="Commodity", y="Profit", color="Commodity",
                          title="Expected Profit by Commodity", text_auto=True,
                          color_discrete_sequence=[GREEN, AMBER, BLUE, "#8B5E3C", RED])
            fig2.update_traces(texttemplate="₦%{y:,.0f}", textposition="outside")
            fig2.update_layout(showlegend=False, height=360,
                               margin=dict(l=0, r=0, t=40, b=0),
                               yaxis_title="Profit (₦)")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Run the optimizer to see profit breakdown.")

    st.divider()
    st.subheader("🗺️ North vs South Price Gap")
    gap = query("""
        SELECT c.name AS commodity,
               AVG(CASE WHEN s.zone = 'North' THEN cp.price_per_unit END) AS north_avg,
               AVG(CASE WHEN s.zone = 'South' THEN cp.price_per_unit END) AS south_avg
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  cp.price_date >= (CURRENT_DATE - INTERVAL '7 days')
          AND  cp.is_outlier IS NOT TRUE
        GROUP BY c.name
        HAVING AVG(CASE WHEN s.zone='North' THEN cp.price_per_unit END) IS NOT NULL
           AND AVG(CASE WHEN s.zone='South' THEN cp.price_per_unit END) IS NOT NULL
    """)
    if not gap.empty:
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(y=gap["commodity"], x=gap["north_avg"],
                              name="North avg (buy)", orientation="h", marker_color=BLUE))
        fig3.add_trace(go.Bar(y=gap["commodity"], x=gap["south_avg"],
                              name="South avg (sell)", orientation="h", marker_color=GREEN))
        fig3.update_layout(barmode="group",
                           title="Avg Price: North (supply) vs South (demand) — Last 7 Days",
                           height=320, margin=dict(l=0, r=0, t=40, b=0),
                           xaxis_title="Price (₦/unit)",
                           legend=dict(orientation="h", y=-0.3))
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Not enough zonal price data yet. Agents need to submit across more states.")

    st.divider()
    st.subheader("🔧 Recent Pipeline Activity")
    logs = query("""
        SELECT run_type AS "Pipeline", status AS "Status",
               records_in AS "In", records_out AS "Out",
               ROUND(duration_secs::numeric, 1) AS "Duration (s)",
               run_at AS "Timestamp"
        FROM   pipeline_logs
        ORDER BY run_at DESC LIMIT 12
    """)
    if not logs.empty:
        logs["Status"] = logs["Status"].apply(
            lambda s: f"✅ {s}" if s == "Success"
                      else f"❌ {s}" if s == "Failed"
                      else f"⚠️ {s}"
        )
        st.dataframe(logs, use_container_width=True, hide_index=True)
    else:
        st.info("No pipeline runs recorded yet.")


# ══════════════════════════════════════════════════════════
# TAB 2 — RECOMMENDATIONS
# ══════════════════════════════════════════════════════════

elif tab == "🚚 Recommendations":
    st.title("🚚 Weekly Trade Recommendations")
    st.caption("Ranked by profit. Each route = what to buy, where, where to sell, expected return.")

    runs = query("""
        SELECT id, run_date, week_start, week_end,
               solver_status, total_profit_ngn
        FROM   optimization_runs
        ORDER BY run_date DESC LIMIT 10
    """)
    if runs.empty:
        st.warning("No optimization runs yet. Go to ⚙️ Data Management → Run Optimization Pipeline.")
        st.stop()

    run_labels = [
        f"Run {r['id']} — {r['run_date']}  |  Profit: {naira(r['total_profit_ngn'])}"
        for _, r in runs.iterrows()
    ]
    sel_idx    = st.selectbox("Optimization run:", range(len(run_labels)),
                               format_func=lambda i: run_labels[i])
    sel_run_id = int(runs.iloc[sel_idx]["id"])
    recs       = load_recs_full(sel_run_id)

    if recs.empty:
        st.info("No recommendations for this run.")
        st.stop()

    # Safe boolean coercion
    for col in ["is_shock_flagged", "is_backhaul"]:
        recs[col] = pd.to_numeric(recs[col], errors="coerce").fillna(0).astype(int)
    recs["no_cost"] = recs["transport_cost"].fillna(0) == 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("💰 Total Expected Profit", naira(recs["expected_profit_ngn"].sum()))
    k2.metric("🚚 Routes",                len(recs))
    k3.metric("⚠️ High-Risk",             int(recs["is_shock_flagged"].sum()))
    k4.metric("📈 Avg Margin",             pct(recs["profit_margin_pct"].mean()))

    st.info(
        "**Buy** at origin · **Transport** · **Sell** at destination. "
        "⚠️ NO COST = no transport data. ⚠️ RISK = uncertain forecast."
    )

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        comm_filter = st.multiselect("Filter commodity",
                                     recs["commodity"].unique().tolist())
    with fc2:
        risk_only = st.checkbox("High-risk only")
    with fc3:
        back_only = st.checkbox("Backhaul only")

    filtered = recs.copy()
    if comm_filter:
        filtered = filtered[filtered["commodity"].isin(comm_filter)]
    if risk_only:
        filtered = filtered[filtered["is_shock_flagged"] == 1]
    if back_only:
        filtered = filtered[filtered["is_backhaul"] == 1]

    st.caption(f"Showing **{len(filtered)}** of {len(recs)} recommendations")
    st.divider()

    for _, row in filtered.iterrows():
        is_risk = row["is_shock_flagged"] == 1
        is_back = row["is_backhaul"] == 1
        no_cost = bool(row["no_cost"])
        profit  = float(row["expected_profit_ngn"])
        margin  = float(row["profit_margin_pct"])
        dist    = row.get("distance_km")
        road    = str(row.get("road_quality") or "")
        perish  = str(row.get("perishability_class") or "")

        flags = []
        if is_risk:  flags.append("⚠️ HIGH RISK")
        if no_cost:  flags.append("⚠️ NO COST DATA")
        if is_back:  flags.append("↩ BACKHAUL")
        if perish == "Perishable": flags.append("⚡ PERISHABLE")
        route_note = f"  ·  🛣️ {dist:.0f}km · {road} road" if dist else ""

        with st.container(border=True):
            h1, h2 = st.columns([3, 1])
            with h1:
                st.markdown(f"### 🌾 {row['commodity']}")
                st.markdown(f"📍 **{row['origin']}** → **{row['destination']}**{route_note}")
            with h2:
                for f in flags:
                    st.warning(f)

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Quantity",       f"{row['recommended_quantity']:.0f} units")
            c2.metric("Buy at",         naira(row["buy_price"]))
            c3.metric("Sell at",        naira(row["sell_price"]))
            c4.metric("Transport/unit", naira(row["transport_cost"]))
            c5.metric("Profit/unit",    naira(row["profit_per_unit"]))
            c6.metric("Total Profit",   naira(profit))
        st.write("")

    export_df = filtered[[
        "commodity","origin","destination","recommended_quantity",
        "buy_price","sell_price","transport_cost",
        "profit_per_unit","expected_profit_ngn","profit_margin_pct"
    ]].copy()
    export_df.columns = [
        "Commodity","Origin","Destination","Quantity",
        "Buy Price","Sell Price","Transport Cost",
        "Profit/Unit","Total Profit","Margin %"
    ]
    st.download_button(
        "⬇️ Export CSV",
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
        "Profit map — expected profit per unit for every origin→destination corridor. "
        "Darkest green = best corridor. Red = unprofitable."
    )

    comm_list = query("SELECT DISTINCT name FROM commodities ORDER BY name")["name"].tolist()
    sel       = st.selectbox("Commodity:", comm_list)

    tableau = query("""
        SELECT s_orig.name AS origin, s_dest.name AS destination,
               cp.price_per_unit                  AS buy_price,
               f.predicted_price                  AS sell_price,
               COALESCE(tc.cost_per_unit, 0)      AS transport_cost,
               (f.predicted_price - cp.price_per_unit
                - COALESCE(tc.cost_per_unit, 0))  AS profit_per_unit,
               CASE WHEN tc.cost_per_unit IS NULL THEN 1 ELSE 0 END AS missing_cost,
               COALESCE(f.is_shock_flagged, FALSE) AS is_shock_flagged
        FROM   corridors corr
        JOIN   states s_orig ON corr.origin_state_id = s_orig.id
        JOIN   states s_dest ON corr.dest_state_id   = s_dest.id
        JOIN   commodities c ON c.name = ?
        JOIN   cleaned_prices cp
               ON  cp.state_id     = corr.origin_state_id
               AND cp.commodity_id = c.id
               AND cp.price_date   = (
                   SELECT MAX(p2.price_date)
                   FROM   cleaned_prices p2
                   WHERE  p2.state_id     = corr.origin_state_id
                     AND  p2.commodity_id = c.id
                     AND  p2.is_outlier IS NOT TRUE
               )
        JOIN   forecasts f
               ON  f.state_id      = corr.dest_state_id
               AND f.commodity_id  = c.id
               AND f.forecast_date = (
                   SELECT MAX(f2.forecast_date)
                   FROM   forecasts f2
                   WHERE  f2.state_id     = corr.dest_state_id
                     AND  f2.commodity_id = c.id
               )
               AND f.generated_on  = (
                   SELECT MAX(f3.generated_on)
                   FROM   forecasts f3
                   WHERE  f3.state_id     = corr.dest_state_id
                     AND  f3.commodity_id = c.id
               )
        LEFT JOIN transport_costs tc
               ON  tc.corridor_id  = corr.id
               AND tc.commodity_id = c.id
               AND (tc.expiry_date IS NULL OR tc.expiry_date >= CURRENT_DATE)
        WHERE  corr.is_active = 1
    """, (sel,))

    if tableau.empty:
        st.info(f"No tableau data for **{sel}** yet. Run the forecasting pipeline first.")
    else:
        pivot = tableau.pivot_table(
            index="origin", columns="destination",
            values="profit_per_unit", aggfunc="mean"
        )
        fig = go.Figure(go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0.0, RED], [0.5, "#F7DC6F"], [1.0, GREEN]],
            zmid=0,
            text=[[f"₦{v:,.0f}" if not np.isnan(v) else "—"
                   for v in r] for r in pivot.values],
            texttemplate="%{text}",
            hovertemplate="<b>%{y} → %{x}</b><br>₦%{z:,.0f}/unit<extra></extra>",
            colorbar=dict(title="Profit/Unit (₦)"),
        ))
        fig.update_layout(
            title=f"{sel} — Profit/Unit · Row=Origin · Column=Destination",
            height=460, margin=dict(l=0, r=0, t=50, b=0),
            xaxis_title="Destination", yaxis_title="Origin",
        )
        st.plotly_chart(fig, use_container_width=True)

        best = tableau[tableau["profit_per_unit"] > 0].nlargest(3, "profit_per_unit")
        if not best.empty:
            st.subheader("🏆 Top 3 Routes")
            for i, (_, r) in enumerate(best.iterrows(), 1):
                note = " *(no transport cost)*" if r["missing_cost"] else ""
                st.markdown(
                    f"**{i}. {r['origin']} → {r['destination']}** — "
                    f"**{naira(r['profit_per_unit'])}/unit** "
                    f"(Buy: {naira(r['buy_price'])}, Sell: {naira(r['sell_price'])}, "
                    f"Transport: {naira(r['transport_cost'])}){note}"
                )

        with st.expander("📋 Full breakdown table"):
            d = tableau.copy()
            for col in ["buy_price","sell_price","transport_cost","profit_per_unit"]:
                d[col] = d[col].apply(naira)
            d["missing_cost"]     = d["missing_cost"].apply(lambda x: "⚠ Est." if x else "✓ Real")
            d["is_shock_flagged"] = d["is_shock_flagged"].apply(lambda x: "⚠ Uncertain" if x else "✓ Stable")
            st.dataframe(d.rename(columns={
                "origin":"Origin","destination":"Destination",
                "buy_price":"Buy","sell_price":"Sell Forecast",
                "transport_cost":"Transport","profit_per_unit":"Profit/Unit",
                "missing_cost":"Cost Data","is_shock_flagged":"Forecast",
            })[["Origin","Destination","Buy","Sell Forecast",
                "Transport","Profit/Unit","Cost Data","Forecast"]],
            use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════
# TAB 4 — FORECASTS
# ══════════════════════════════════════════════════════════

elif tab == "📈 Forecasts":
    st.title("📈 Price Forecasts")
    st.caption(
        "Prophet's 7-day predictions. "
        "Green = historical. Orange dashed = forecast. "
        "Shaded = 80% confidence band. Red ✕ = high-risk days."
    )

    c1, c2 = st.columns(2)
    with c1:
        sel_comm  = st.selectbox(
            "Commodity",
            query("SELECT DISTINCT name FROM commodities ORDER BY name")["name"].tolist()
        )
    with c2:
        sel_state = st.selectbox(
            "State",
            query("SELECT DISTINCT name FROM states ORDER BY name")["name"].tolist()
        )

    fc = query("""
        SELECT f.forecast_date, f.predicted_price,
               f.lower_bound, f.upper_bound,
               COALESCE(f.is_shock_flagged, FALSE) AS is_shock_flagged,
               f.shock_reason
        FROM   forecasts f
        JOIN   states      s ON f.state_id     = s.id
        JOIN   commodities c ON f.commodity_id = c.id
        WHERE  s.name = ? AND c.name = ?
          AND  f.generated_on = CURRENT_DATE
        ORDER BY f.forecast_date
    """, (sel_state, sel_comm))

    hist = query("""
        SELECT cp.price_date     AS date,
               cp.price_per_unit AS price
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  s.name = ? AND c.name = ?
          AND  cp.price_date >= (CURRENT_DATE - INTERVAL '56 days')
          AND  cp.is_outlier IS NOT TRUE
        ORDER BY cp.price_date
    """, (sel_state, sel_comm))

    if fc.empty and hist.empty:
        st.info(f"No data for **{sel_comm}** in **{sel_state}**. Run forecasting pipeline first.")
    else:
        fig = go.Figure()
        if not hist.empty:
            fig.add_trace(go.Scatter(
                x=hist["date"], y=hist["price"],
                mode="lines+markers", name="Historical",
                line=dict(color=GREEN, width=2), marker=dict(size=4),
                hovertemplate="Date: %{x}<br>Actual: ₦%{y:,.0f}<extra></extra>"
            ))
        if not fc.empty:
            fc["is_shock_flagged"] = pd.to_numeric(
                fc["is_shock_flagged"], errors="coerce").fillna(0)
            fig.add_trace(go.Scatter(
                x=pd.concat([fc["forecast_date"], fc["forecast_date"][::-1]]),
                y=pd.concat([fc["upper_bound"],   fc["lower_bound"][::-1]]),
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
        fig.add_vline(
            x=pd.Timestamp(date.today()).timestamp()*1000,
            line_dash="dot", line_color=GRAY, annotation_text="Today"
        )
        fig.update_layout(
            title=f"{sel_comm} — {sel_state}",
            xaxis_title="Date", yaxis_title="Price (₦/unit)",
            height=440, margin=dict(l=0, r=0, t=50, b=0),
            legend=dict(orientation="h", y=-0.2), hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

        if not fc.empty:
            cs1, cs2, cs3 = st.columns(3)
            cs1.metric("Next Week Avg", naira(fc["predicted_price"].mean()))
            cs2.metric("Price Range",
                       f"{naira(fc['lower_bound'].min())} – {naira(fc['upper_bound'].max())}")
            cs3.metric("High-Risk Days",
                       int(fc["is_shock_flagged"].sum()))

            shocks = fc[fc["is_shock_flagged"] == 1]
            if not shocks.empty:
                st.warning(f"⚠️ {len(shocks)} high-risk day(s). Proceed with caution.")
                with st.expander("Why are these days flagged?"):
                    st.dataframe(
                        shocks[["forecast_date","predicted_price",
                                "lower_bound","upper_bound","shock_reason"]],
                        use_container_width=True, hide_index=True
                    )

            with st.expander("📋 Full 7-day forecast table"):
                fd = fc.copy()
                for c in ["predicted_price","lower_bound","upper_bound"]:
                    fd[c] = fd[c].apply(naira)
                fd["is_shock_flagged"] = fd["is_shock_flagged"].apply(
                    lambda x: "⚠ High Risk" if x else "✓ Normal"
                )
                st.dataframe(
                    fd[["forecast_date","predicted_price","lower_bound",
                        "upper_bound","is_shock_flagged"]].rename(columns={
                        "forecast_date":"Date",
                        "predicted_price":"Predicted",
                        "lower_bound":"Low",
                        "upper_bound":"High",
                        "is_shock_flagged":"Risk",
                    }),
                    use_container_width=True, hide_index=True
                )


# ══════════════════════════════════════════════════════════
# TAB 5 — FEEDBACK
# ══════════════════════════════════════════════════════════

elif tab == "📝 Feedback":
    st.title("📝 Log Actual Trade Outcomes")
    st.caption("Record what actually happened. This teaches the system and improves future forecasts.")

    pending = query("""
        SELECT r.id, co.name AS commodity,
               s_orig.name AS origin, s_dest.name AS destination,
               r.recommended_quantity, r.buy_price,
               r.sell_price, r.expected_profit_ngn, r.status
        FROM   optimization_recommendations r
        JOIN   commodities co ON r.commodity_id = co.id
        LEFT JOIN corridors corr ON r.corridor_id       = corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id = s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id   = s_dest.id
        WHERE  r.status = 'Pending'
        ORDER BY r.id DESC LIMIT 30
    """)

    if pending.empty:
        st.info("No pending recommendations. All completed, or no optimization run yet.")
    else:
        rec_labels = [
            f"#{r['id']} — {r['commodity']} | {r['origin']} → {r['destination']} "
            f"(predicted: {naira(r['expected_profit_ngn'])})"
            for _, r in pending.iterrows()
        ]
        sel_idx = st.selectbox("Select completed trade:", range(len(rec_labels)),
                               format_func=lambda i: rec_labels[i])
        sel = pending.iloc[sel_idx]

        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("Predicted buy",      naira(sel["buy_price"]))
        pc2.metric("Predicted sell",     naira(sel["sell_price"]))
        pc3.metric("Recommended qty",    f"{sel['recommended_quantity']:.0f}")
        pc4.metric("Predicted profit",   naira(sel["expected_profit_ngn"]))

        with st.form("feedback_form"):
            fc1, fc2 = st.columns(2)
            with fc1:
                actual_buy       = st.number_input("Actual buy price (₦/unit)",
                                                   value=float(sel["buy_price"] or 0), step=100.0)
                actual_sell      = st.number_input("Actual sell price (₦/unit)",
                                                   value=float(sel["sell_price"] or 0), step=100.0)
                actual_qty       = st.number_input("Units moved",
                                                   value=float(sel["recommended_quantity"] or 0), step=1.0)
            with fc2:
                actual_transport = st.number_input("Actual transport cost (₦/unit)", value=0.0, step=100.0)
                trip_date        = st.date_input("Date of trade", value=date.today())
                notes            = st.text_area("Notes (optional)",
                                                placeholder="e.g. Road was bad, prices higher than expected...")

            if actual_sell > 0:
                preview  = actual_qty * (actual_sell - actual_buy - actual_transport)
                diff     = preview - float(sel["expected_profit_ngn"] or 0)
                diff_str = f"▲ {naira(diff)} more" if diff >= 0 else f"▼ {naira(abs(diff))} less"
                st.info(f"📊 **Actual profit preview:** {naira(preview)} ({diff_str} than predicted)")

            if st.form_submit_button("✅ Submit Outcome", type="primary"):
                actual_profit = actual_qty * (actual_sell - actual_buy - actual_transport)
                try:
                    # Use db_adapter.execute — NOT conn.cursor()
                    execute("""
                        INSERT INTO actual_outcomes (
                            recommendation_id, commodity_id, corridor_id,
                            actual_buy_price, actual_sell_price,
                            actual_transport_cost, actual_quantity,
                            actual_profit_ngn, trip_date,
                            outcome_notes, data_source
                        )
                        SELECT ?, r.commodity_id, r.corridor_id,
                               ?, ?, ?, ?, ?,
                               ?, ?, 'Dashboard'
                        FROM optimization_recommendations r
                        WHERE r.id = ?
                    """, (
                        int(sel["id"]),
                        actual_buy, actual_sell, actual_transport,
                        actual_qty, round(actual_profit, 2),
                        str(trip_date), notes,
                        int(sel["id"]),
                    ))
                    execute(
                        "UPDATE optimization_recommendations SET status = ? WHERE id = ?",
                        ("Completed", int(sel["id"]))
                    )
                    st.success(f"✅ Saved! Actual profit: **{naira(actual_profit)}**")
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ Could not save: {e}")

    st.divider()
    st.subheader("📚 Previously Logged Outcomes")
    outcomes = query("""
        SELECT ao.trip_date          AS date,
               c.name                AS commodity,
               ao.actual_buy_price   AS buy_price,
               ao.actual_sell_price  AS sell_price,
               ao.actual_transport_cost AS transport,
               ao.actual_quantity    AS quantity,
               ao.actual_profit_ngn  AS profit,
               ao.outcome_notes      AS notes
        FROM   actual_outcomes ao
        JOIN   commodities c ON ao.commodity_id = c.id
        ORDER  BY ao.trip_date DESC LIMIT 20
    """)
    if not outcomes.empty:
        for col in ["buy_price","sell_price","transport","profit"]:
            outcomes[col] = outcomes[col].apply(naira)
        st.dataframe(outcomes, use_container_width=True, hide_index=True)
    else:
        st.info("No outcomes logged yet.")


# ══════════════════════════════════════════════════════════
# TAB 6 — DATA MANAGEMENT
# ══════════════════════════════════════════════════════════

elif tab == "⚙️ Data Management":
    st.title("⚙️ Data Management")
    st.caption("The engine room — run pipelines, register agents, upload data, inspect the database.")
    st.info(
        "**Run in order every week:** "
        "Step 1 (Clean) → Step 2 (Forecast) → Step 3 (Optimize)."
    )

    # ── Pipelines ─────────────────────────────────────────
    st.subheader("🔄 Run Pipelines")

    with st.container(border=True):
        st.markdown("**Step 1 — 🧹 Cleaning Pipeline**")
        st.caption("Reads raw agent submissions, removes outliers, saves clean prices.")
        if st.button("▶ Run Cleaning Pipeline", key="run_clean"):
            with st.spinner("Cleaning..."):
                try:
                    from cleaning import run_cleaning_pipeline
                    run_cleaning_pipeline(source="raw")
                    st.success("✅ Cleaning complete.")
                except Exception as e:
                    st.error(f"❌ {e}")

    with st.container(border=True):
        st.markdown("**Step 2 — 📈 Forecasting Pipeline**")
        st.caption("Trains Prophet on cleaned prices. Takes 2–3 minutes.")
        if st.button("▶ Run Forecasting Pipeline", key="run_fc"):
            with st.spinner("Training forecast models..."):
                try:
                    from forecasting import run_forecasting_pipeline
                    run_forecasting_pipeline(periods=7)
                    st.success("✅ Forecasting complete.")
                except Exception as e:
                    st.error(f"❌ {e}")

    with st.container(border=True):
        st.markdown("**Step 3 — 🚚 Optimization Pipeline**")
        st.caption("Finds most profitable routes. Results appear in Recommendations tab.")
        if st.button("▶ Run Optimization Pipeline", key="run_opt"):
            with st.spinner("Finding best routes..."):
                try:
                    from optimization import run_optimization_pipeline
                    run_optimization_pipeline()
                    st.success("✅ Optimization complete. View results in 🚚 Recommendations.")
                except Exception as e:
                    st.error(f"❌ {e}")

    st.divider()

    # ── Register Agent ─────────────────────────────────────
    st.subheader("👤 Register New Agent")
    st.caption("Adds a new agent to the system. They log in with Agent ID + password.")

    with st.form("register_agent"):
        col1, col2 = st.columns(2)
        with col1:
            a_name     = st.text_input("Full name *")
            a_agent_id = st.text_input("Agent ID *",
                                        placeholder="e.g. TFN-KW-020",
                                        help="Format: TFN-[State Code]-[Number]")
            a_phone    = st.text_input("Phone number", placeholder="08012345678")
        with col2:
            states  = query("SELECT id, name FROM states ORDER BY name")
            markets = query("SELECT id, name FROM markets ORDER BY name")
            a_state = st.selectbox("State *", states["name"].tolist())
            a_market= st.selectbox("Assigned Market",
                                   ["— None —"] + markets["name"].tolist())
            a_pwd   = st.text_input("Password *",
                                     placeholder="They will use this to log in",
                                     type="password")
            a_role  = st.selectbox("Role",
                                   ["Market Agent","State Coordinator","Spot Reporter"])

        if st.form_submit_button("Register Agent", type="primary"):
            if not a_name or not a_agent_id or not a_pwd:
                st.error("Full name, Agent ID, and Password are required.")
            else:
                state_id  = int(states[states["name"] == a_state].iloc[0]["id"])
                market_id = None
                if a_market != "— None —":
                    mrow = markets[markets["name"] == a_market]
                    if not mrow.empty:
                        market_id = int(mrow.iloc[0]["id"])
                try:
                    execute("""
                        INSERT INTO agents
                            (full_name, agent_id, password, phone,
                             state_id, market_id, role, is_active)
                        VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                    """, (a_name.strip(), a_agent_id.strip().upper(),
                          a_pwd, a_phone, state_id, market_id, a_role))
                    st.success(
                        f"✅ **{a_name}** registered as `{a_agent_id.upper()}`. "
                        f"They can log in using their Agent ID and password."
                    )
                except Exception as e:
                    err = str(e).lower()
                    if "unique" in err or "duplicate" in err:
                        st.error(f"❌ Agent ID `{a_agent_id}` already exists. Choose a different ID.")
                    else:
                        st.error(f"❌ {e}")

    st.divider()

    # ── CSV Upload ─────────────────────────────────────────
    st.subheader("📂 Upload Agent Price Reports")
    import tempfile  # ensure available in both columns
    cu1, cu2 = st.columns([3, 1])
    with cu1:
        uploaded = st.file_uploader("Upload CSV or Excel", type=["csv","xlsx"])
        if uploaded:
            suffix = ".csv" if uploaded.name.endswith(".csv") else ".xlsx"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded.read()); tmp_path = tmp.name
            try:
                from csv_uploader import ingest_csv
                ok = ingest_csv(tmp_path)
                if ok:
                    st.success("✅ Uploaded. Now run the Cleaning Pipeline above.")
                else:
                    st.error("❌ Format issue. Check columns match the template.")
            except Exception as e:
                st.error(f"❌ {e}")
    with cu2:
        st.markdown("**Need the template?**")
        try:
            from csv_uploader import generate_template
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w') as tmp2:
                tmp2_name = tmp2.name
            generate_template(tmp2_name)
            with open(tmp2_name) as f:
                tpl = f.read()
            st.download_button("⬇️ Template CSV", data=tpl,
                               file_name="agent_template.csv", mime="text/csv")
        except Exception as e:
            st.error(f"❌ {e}")

    st.divider()

    # ── DB Inspector ───────────────────────────────────────
    st.subheader("🗄️ Database Inspector")
    tables = [
        "states","markets","commodities","agents","corridors",
        "raw_submissions","cleaned_prices","transport_costs",
        "forecasts","optimization_runs",
        "optimization_recommendations","actual_outcomes","pipeline_logs"
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
        "Status":  ("✅ Has data" if isinstance(counts[t],int) and counts[t] > 0
                    else "⚪ Empty" if counts[t] == 0
                    else "❌ Error"),
    } for t in tables])
    st.dataframe(cdf, use_container_width=True, hide_index=True)

    sel_table = st.selectbox("Preview table:", tables)
    n_rows    = st.slider("Rows to show", 5, 50, 10)
    try:
        preview = query(f"SELECT * FROM {sel_table} LIMIT {n_rows}")
        st.dataframe(preview, use_container_width=True, hide_index=True)
        st.caption(f"{len(preview)} of {counts[sel_table]} records in `{sel_table}`")
    except Exception as e:
        st.error(f"Could not load: {e}")
