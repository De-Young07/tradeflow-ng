"""
TradeFlow NG — Agent Dashboard v2
Mobile-first, interactive, energetic design.
Animations, tap feedback, and clear actionable trade cards.
"""

import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime
import sys, os

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from auth import require_agent_login, agent_logout

LOGO_FULL = os.path.join(os.path.dirname(__file__), 'assets', 'TradeFlow profile.png')
LOGO_ICON = os.path.join(os.path.dirname(__file__), 'assets', 'TradeFlow logo.png')

st.set_page_config(
    page_title="TradeFlow NG — Agent",
    page_icon=LOGO_ICON,
    layout="centered",
)

DB_PATH = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"

GREEN  = "#1A6B3C"
LIME   = "#2ECC71"
AMBER  = "#F39C12"
RED    = "#E74C3C"
DARK   = "#1A1A2E"
GRAY   = "#7F8C8D"
LIGHT  = "#E8F8F0"

# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════

@st.cache_resource
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

def query(sql, params=()):
    return pd.read_sql(sql, get_connection(), params=params)

def naira(v):
    try: return f"₦{float(v):,.0f}"
    except: return "₦—"

# ══════════════════════════════════════════════════════════
# CSS — ANIMATED, MOBILE-FIRST
# ══════════════════════════════════════════════════════════

st.markdown(f"""
<style>
/* Force form inputs to always be readable */
.stTextInput input,
.stNumberInput input,
.stTextArea textarea,
.stSelectbox > div > div > div,
.stDateInput input {{
    background-color: white !important;
    color: #1A1A1A !important;
    border: 1.5px solid #D5E8DC !important;
    border-radius: 10px !important;
}}

/* Force labels above inputs to be visible */
.stTextInput label,
.stNumberInput label,
.stTextArea label,
.stSelectbox label,
.stDateInput label {{
    color: white !important;
    font-weight: 600 !important;
}}

/* Selectbox dropdown text */
.stSelectbox [data-baseweb="select"] {{
    background: white !important;
}}
.stSelectbox [data-baseweb="select"] * {{
    color: #1A1A1A !important;
}}    

@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {{
    font-family: 'Plus Jakarta Sans', sans-serif;
}}

/* ── Page background ── */
.stApp {{
    background: linear-gradient(135deg, #0a2e1a 0%, #0f3d22 40%, #1a5c35 100%);
    min-height: 100vh;
}}

/* ── Animated hero header ── */
@keyframes slideDown {{
    from {{ opacity:0; transform:translateY(-20px); }}
    to   {{ opacity:1; transform:translateY(0); }}
}}
@keyframes pulse {{
    0%, 100% {{ transform: scale(1); }}
    50%       {{ transform: scale(1.05); }}
}}
@keyframes fadeIn {{
    from {{ opacity:0; transform:translateY(12px); }}
    to   {{ opacity:1; transform:translateY(0); }}
}}
@keyframes shimmer {{
    0%   {{ background-position: -200% center; }}
    100% {{ background-position:  200% center; }}
}}

.hero {{
    background: linear-gradient(135deg, #1A6B3C 0%, #0d4a27 100%);
    border-radius: 20px;
    padding: 28px 24px 22px;
    margin-bottom: 20px;
    text-align: center;
    color: white;
    animation: slideDown 0.6s ease-out;
    position: relative;
    overflow: hidden;
    box-shadow: 0 8px 32px rgba(0,0,0,0.3);
}}
.hero::before {{
    content: '';
    position: absolute;
    top: -50%; left: -50%;
    width: 200%; height: 200%;
    background: linear-gradient(
        90deg,
        transparent 0%,
        rgba(255,255,255,0.06) 50%,
        transparent 100%
    );
    background-size: 200% 100%;
    animation: shimmer 3s infinite;
}}
.hero-icon  {{ font-size: 3rem; animation: pulse 2s infinite; }}
.hero-name  {{ font-size: 1.5rem; font-weight: 800; margin: 8px 0 4px; }}
.hero-sub   {{ font-size: 0.88rem; opacity: 0.8; }}
.hero-badge {{
    display: inline-block;
    background: rgba(255,255,255,0.15);
    border: 1px solid rgba(255,255,255,0.25);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: 0.78rem;
    margin-top: 10px;
    backdrop-filter: blur(4px);
}}

/* ── Nav tabs ── */
.stRadio > div {{
    display: flex; gap: 8px;
    background: rgba(255,255,255,0.08);
    border-radius: 14px; padding: 6px;
    backdrop-filter: blur(8px);
    margin-bottom: 16px;
}}
.stRadio label {{
    flex: 1; text-align: center;
    background: transparent;
    border-radius: 10px;
    padding: 10px 6px !important;
    cursor: pointer;
    color: rgba(255,255,255,0.7) !important;
    font-weight: 600;
    font-size: 0.85rem;
    transition: all 0.25s ease !important;
    border: none !important;
}}
.stRadio label:hover {{
    background: rgba(255,255,255,0.12) !important;
    color: white !important;
}}
[data-baseweb="radio"] input:checked + div + label,
.stRadio [aria-checked="true"] {{
    background: white !important;
    color: {GREEN} !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.2);
}}

/* ── Trade cards ── */
@keyframes cardIn {{
    from {{ opacity:0; transform:translateY(16px); }}
    to   {{ opacity:1; transform:translateY(0); }}
}}
.trade-card {{
    background: rgba(255,255,255,0.97);
    border-radius: 18px;
    padding: 20px;
    margin-bottom: 16px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.15);
    animation: cardIn 0.4s ease-out both;
    border: 1px solid rgba(255,255,255,0.5);
    transition: transform 0.2s, box-shadow 0.2s;
}}
.trade-card:hover {{
    transform: translateY(-3px);
    box-shadow: 0 8px 30px rgba(0,0,0,0.2);
}}
.trade-card.urgent {{ border-top: 5px solid {AMBER}; }}
.trade-card.normal {{ border-top: 5px solid {LIME}; }}

.trade-tag {{
    display: inline-block;
    padding: 3px 12px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 700;
    margin-bottom: 8px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}
.tag-green {{ background: #D4EDDA; color: #155724; }}
.tag-amber {{ background: #FFF3CD; color: #856404; }}
.tag-blue  {{ background: #D6EAF8; color: #1A5276; }}

.trade-commodity {{
    font-size: 1.3rem;
    font-weight: 800;
    color: {DARK};
    margin-bottom: 2px;
}}
.trade-route {{
    font-size: 0.9rem;
    color: {GRAY};
    margin-bottom: 14px;
    display: flex;
    align-items: center;
    gap: 6px;
}}

.stats-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-bottom: 14px;
}}
.stat-box {{
    background: #F8FAF9;
    border-radius: 12px;
    padding: 12px;
    border: 1px solid #E8EEE9;
}}
.stat-label {{
    font-size: 0.68rem;
    color: #999;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 4px;
    font-weight: 600;
}}
.stat-value {{
    font-size: 1.1rem;
    font-weight: 700;
    color: {DARK};
}}
.stat-value.buy   {{ color: {RED}; }}
.stat-value.sell  {{ color: {GREEN}; }}

.profit-banner {{
    background: linear-gradient(135deg, {GREEN} 0%, #0d4a27 100%);
    border-radius: 14px;
    padding: 16px;
    text-align: center;
    color: white;
    margin-top: 4px;
    position: relative;
    overflow: hidden;
}}
.profit-banner::after {{
    content: '₦';
    position: absolute;
    right: -10px; top: -10px;
    font-size: 5rem;
    opacity: 0.08;
    font-weight: 900;
}}
.profit-label {{ font-size: 0.75rem; opacity: 0.8; text-transform: uppercase; }}
.profit-value {{
    font-size: 1.8rem;
    font-weight: 800;
    letter-spacing: -0.02em;
}}
.profit-margin {{ font-size: 0.8rem; opacity: 0.75; margin-top: 2px; }}

.warn-strip {{
    background: #FFF8E6;
    border: 1px solid #FFD980;
    border-radius: 10px;
    padding: 8px 12px;
    font-size: 0.8rem;
    color: #7A5C00;
    margin-top: 10px;
    display: flex; gap: 8px; align-items: center;
}}

/* ── No trades ── */
.no-trades {{
    background: rgba(255,255,255,0.08);
    border-radius: 18px;
    padding: 48px 24px;
    text-align: center;
    color: rgba(255,255,255,0.7);
    border: 1px solid rgba(255,255,255,0.12);
}}

/* ── Section headers ── */
.section-hdr {{
    font-size: 0.8rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: rgba(255,255,255,0.5);
    margin: 24px 0 12px;
}}

/* ── Forms ── */
.stForm {{
    background: rgba(255,255,255,0.97) !important;
    border-radius: 18px !important;
    padding: 20px !important;
    box-shadow: 0 4px 20px rgba(0,0,0,0.15) !important;
    animation: cardIn 0.4s ease-out;
}}
.stTextInput input, .stNumberInput input, .stTextArea textarea,
.stSelectbox > div > div {{
    border-radius: 10px !important;
    border: 1.5px solid #E0EEE6 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    background: #F8FAF9 !important;
    font-size: 0.95rem !important;
    transition: border-color 0.2s !important;
}}
.stTextInput input:focus, .stNumberInput input:focus,
.stTextArea textarea:focus {{
    border-color: {GREEN} !important;
    box-shadow: 0 0 0 3px rgba(26,107,60,0.12) !important;
    background: white !important;
}}

/* ── Buttons ── */
.stButton > button {{
    border-radius: 12px !important;
    font-weight: 700 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    transition: all 0.2s !important;
    border: none !important;
    letter-spacing: 0.02em !important;
}}
.stButton > button[kind="primary"] {{
    background: linear-gradient(135deg, {GREEN} 0%, #0d4a27 100%) !important;
    color: white !important;
    padding: 14px 24px !important;
    font-size: 1rem !important;
    width: 100% !important;
    box-shadow: 0 4px 14px rgba(26,107,60,0.4) !important;
}}
.stButton > button[kind="primary"]:hover {{
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 20px rgba(26,107,60,0.5) !important;
}}
.stButton > button[kind="primary"]:active {{
    transform: translateY(0) !important;
}}

/* ── Metrics ── */
[data-testid="metric-container"] {{
    background: rgba(255,255,255,0.97);
    border-radius: 14px;
    border: none;
    border-top: 3px solid {LIME};
    padding: 14px;
    box-shadow: 0 4px 14px rgba(0,0,0,0.1);
    animation: cardIn 0.4s ease-out;
}}

/* ── Price table ── */
.stDataFrame {{
    background: rgba(255,255,255,0.97);
    border-radius: 14px;
    overflow: hidden;
    box-shadow: 0 4px 14px rgba(0,0,0,0.1);
    animation: cardIn 0.4s ease-out;
}}

/* ── Success/error messages ── */
.stSuccess {{
    border-radius: 12px !important;
    animation: cardIn 0.3s ease-out;
}}

/* ── Login card ── */
.login-card {{
    background: rgba(255,255,255,0.97);
    border-radius: 22px;
    padding: 36px 28px;
    text-align: center;
    box-shadow: 0 12px 40px rgba(0,0,0,0.25);
    animation: slideDown 0.6s ease-out;
    margin-top: 20px;
}}
.login-icon {{ font-size: 3.5rem; margin-bottom: 12px; }}
.login-title {{ font-size: 1.6rem; font-weight: 800; color: {DARK}; margin-bottom: 6px; }}
.login-sub   {{ font-size: 0.9rem; color: {GRAY}; margin-bottom: 24px; }}

/* ── Profit preview box ── */
.preview-box {{
    background: linear-gradient(135deg, #E8F8F0, #D4EDDA);
    border: 1px solid #B8DECC;
    border-radius: 14px;
    padding: 16px;
    text-align: center;
    margin: 12px 0;
    animation: fadeIn 0.3s ease-out;
}}
.preview-label {{ font-size: 0.75rem; color: {GRAY}; font-weight: 600; text-transform: uppercase; }}
.preview-value {{ font-size: 1.6rem; font-weight: 800; color: {GREEN}; }}
.preview-diff  {{ font-size: 0.82rem; margin-top: 4px; }}

/* ── Submission success card ── */
.submit-success {{
    background: linear-gradient(135deg, {GREEN}, #0d4a27);
    color: white; border-radius: 18px;
    padding: 28px; text-align: center;
    animation: cardIn 0.4s ease-out;
    box-shadow: 0 8px 24px rgba(26,107,60,0.4);
}}

#MainMenu, footer, header {{ visibility: hidden; }}
</style>
""", unsafe_allow_html=True)



# ══════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════

for key, default in [
    ("agent_id", None),
    ("agent_name", None),
    ("agent_state", None),
    ("agent_state_id", None),
    ("agent_authenticated", False),  # ← add this if missing
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ══════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════


authenticated, agent_data = require_agent_login()
if not authenticated:
    st.stop()

# Then use agent_data instead of st.session_state directly:
agent_id    = agent_data["agent_id"]
agent_name  = agent_data["agent_name"]
agent_state = agent_data["agent_state"]
sid         = agent_data["agent_state_id"]

# ══════════════════════════════════════════════════════════
# LOGGED IN — VARIABLES
# ══════════════════════════════════════════════════════════
col_logo, col_spacer = st.columns([1, 3])
with col_logo:
    if os.path.exists(LOGO_FULL):
        st.image(LOGO_FULL, width=160)

# ── Hero header ──────────────────────────────────────────

agent_name  = st.session_state.get("agent_name") or "Agent"
agent_state = st.session_state.get("agent_state") or "—"
sid         = st.session_state.get("agent_state_id")
agent_id    = st.session_state.get("agent_id")
first_name  = agent_name.split()[0]
st.markdown(f"""
<div class="hero">
    <div class="hero-icon">👋</div>
    <div class="hero-name">Hello, {first_name}!</div>
    <div class="hero-sub">Your trade assignments for this week</div>
    <div class="hero-badge">📍 {agent_state} &nbsp;|&nbsp; 📅 {date.today().strftime('%d %b %Y')}</div>
</div>
""", unsafe_allow_html=True)

# ── Navigation ────────────────────────────────────────────
page = st.radio(
    "", ["📋 My Trades", "✅ Report", "💬 Price"],
    horizontal=True, label_visibility="collapsed"
)

# ── Logout ────────────────────────────────────────────────
if st.button("Logout"):
    for k in ["agent_id","agent_name","agent_state","agent_state_id"]:
        st.session_state[k] = None
    st.rerun()


# ══════════════════════════════════════════════════════════
# PAGE 1: MY TRADES
# ══════════════════════════════════════════════════════════

if page == "📋 My Trades":

    # Summary KPIs
    recs_all = query("""
        SELECT r.*,
               (r.sell_price - r.buy_price - r.transport_cost) AS profit_per_unit,
               co.name AS commodity, s_orig.name AS origin,
               s_dest.name AS destination,
               corr.distance_km, corr.road_quality,
               c2.perishability_class
        FROM optimization_recommendations r
        JOIN commodities co  ON r.commodity_id=co.id
        JOIN commodities c2  ON r.commodity_id=c2.id
        LEFT JOIN corridors corr ON r.corridor_id=corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id=s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id=s_dest.id
        WHERE r.run_id=(SELECT MAX(id) FROM optimization_runs)
          AND r.status='Pending'
          AND (corr.origin_state_id=? OR corr.dest_state_id=?)
        ORDER BY r.expected_profit_ngn DESC
    """, (sid, sid)) if sid else pd.DataFrame()

    if not recs_all.empty:
        # Safely convert boolean columns
        recs_all["is_shock_flagged"] = pd.to_numeric(recs_all["is_shock_flagged"], errors="coerce").fillna(0).astype(int)
        recs_all["is_backhaul"]      = pd.to_numeric(recs_all["is_backhaul"],      errors="coerce").fillna(0).astype(int)

        c1, c2, c3 = st.columns(3)
        c1.metric("🚚 Trades", len(recs_all))
        c2.metric(
            "💰 Total Potential",
            naira(recs_all["expected_profit_ngn"].sum())
        )
        c3.metric(
            "⚠️ High-Risk",
            int(recs_all["is_shock_flagged"].sum())
        )

    st.markdown(
        '<p class="section-hdr">Your Trade Assignments</p>',
        unsafe_allow_html=True
    )

    if recs_all.empty:
        st.markdown("""
        <div class="no-trades">
            <div style="font-size:3rem;margin-bottom:12px;">📭</div>
            <div style="font-size:1.1rem;font-weight:700;color:white;margin-bottom:8px;">
                No trades yet
            </div>
            <div style="font-size:0.88rem;opacity:0.7;">
                The system hasn't run this week's optimization yet.<br>
                Check back soon or contact your supervisor.
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        for i, (_, row) in enumerate(recs_all.iterrows()):
            is_risk  = int(row.get("is_shock_flagged", 0)) == 1
            no_cost  = float(row.get("transport_cost", 0)) == 0
            is_back  = int(row.get("is_backhaul", 0)) == 1
            perishab = str(row.get("perishability_class",""))
            profit   = float(row.get("expected_profit_ngn", 0))
            margin   = float(row.get("profit_margin_pct", 0))
            card_cls = "urgent" if is_risk else "normal"

            # Build tags
            tags_parts = []
            if not is_risk and not no_cost:
                tags_parts.append('<span class="trade-tag tag-green">Verified</span>')
            if is_risk:
                tags_parts.append('<span class="trade-tag tag-amber">High Risk</span>')
            if is_back:
                tags_parts.append('<span class="trade-tag tag-blue">Backhaul</span>')
            if perishab == "Perishable":
                tags_parts.append('<span class="trade-tag tag-amber">Perishable</span>')
            tags_html = " ".join(tags_parts)

            # Build route detail
            dist         = row.get("distance_km")
            road         = str(row.get("road_quality") or "")
            route_detail = (f"{dist:.0f}km - {road} road" if dist else "")

            # Build warn strips
            warns = []
            if is_risk:
                warns.append('<div class="warn-strip">Price forecast is uncertain this week - proceed carefully</div>')
            if no_cost:
                warns.append('<div class="warn-strip">Transport cost unconfirmed - actual profit may be lower</div>')
            warns_html = "\n".join(warns)

            # Values
            qty_str       = f"{row['recommended_quantity']:.0f} units"
            buy_str       = naira(row['buy_price'])
            sell_str      = naira(row['sell_price'])
            transport_str = naira(row['transport_cost'])
            if no_cost:
                transport_str = transport_str + " *"
            profit_str    = naira(profit)
            margin_str    = f"{margin:.1f}%"
            origin_str    = str(row['origin'])
            dest_str      = str(row['destination'])
            comm_str      = str(row['commodity'])
            delay         = i * 0.08

            # Build complete HTML as a single string — no nested f-strings
            card_html = (
                '<div class="trade-card ' + card_cls + '" style="animation-delay:' + str(delay) + 's;">'
                + tags_html
                + '<div class="trade-commodity">' + comm_str + '</div>'
                + '<div class="trade-route">'
                + origin_str + ' &rarr; ' + dest_str
                + (' <span style="font-size:0.78rem;color:#aaa;">' + route_detail + '</span>' if route_detail else '')
                + '</div>'
                + '<div class="stats-grid">'
                + '<div class="stat-box"><div class="stat-label">Quantity</div><div class="stat-value">' + qty_str + '</div></div>'
                + '<div class="stat-box"><div class="stat-label">Buy at (origin)</div><div class="stat-value buy">' + buy_str + '</div></div>'
                + '<div class="stat-box"><div class="stat-label">Sell at (dest)</div><div class="stat-value sell">' + sell_str + '</div></div>'
                + '<div class="stat-box"><div class="stat-label">Transport/unit</div><div class="stat-value">' + transport_str + '</div></div>'
                + '</div>'
                + '<div class="profit-banner">'
                + '<div class="profit-label">Expected Total Profit</div>'
                + '<div class="profit-value">' + profit_str + '</div>'
                + '<div class="profit-margin">' + margin_str + ' profit margin</div>'
                + '</div>'
                + warns_html
                + '</div>'
            )

            st.markdown(card_html, unsafe_allow_html=True)

    # Local prices
    st.markdown(
        '<p class="section-hdr">Current Prices in Your Area</p>',
        unsafe_allow_html=True
    )
    local = query("""
        SELECT c.name AS Commodity,
               cp.price_per_unit AS Price,
               cp.price_date AS 'Last Updated'
        FROM cleaned_prices cp
        JOIN commodities c ON cp.commodity_id=c.id
        WHERE cp.state_id=?
          AND cp.price_date=(
              SELECT MAX(p2.price_date) FROM cleaned_prices p2
              WHERE p2.state_id=cp.state_id
                AND p2.commodity_id=cp.commodity_id
          )
        ORDER BY c.name
    """, (sid,)) if sid else pd.DataFrame()

    if not local.empty:
        local["Price"] = local["Price"].apply(naira)
        st.dataframe(local, width='stretch', hide_index=True)
    else:
        st.markdown(
            '<div class="no-trades" style="padding:24px;">'
            'No price data recorded for your area yet.'
            '</div>',
            unsafe_allow_html=True
        )


# ══════════════════════════════════════════════════════════
# PAGE 2: REPORT OUTCOME
# ══════════════════════════════════════════════════════════

elif page == "✅ Report":
    st.markdown(
        '<p class="section-hdr">Report a Completed Trade</p>',
        unsafe_allow_html=True
    )

    pending = query("""
        SELECT r.id, co.name AS commodity,
               s_orig.name AS origin, s_dest.name AS destination,
               r.recommended_quantity, r.buy_price,
               r.sell_price, r.expected_profit_ngn
        FROM optimization_recommendations r
        JOIN commodities co ON r.commodity_id=co.id
        LEFT JOIN corridors corr ON r.corridor_id=corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id=s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id=s_dest.id
        WHERE r.run_id=(SELECT MAX(id) FROM optimization_runs)
          AND r.status='Pending'
          AND (corr.origin_state_id=? OR corr.dest_state_id=?)
        ORDER BY r.id DESC
    """, (sid, sid)) if sid else pd.DataFrame()

    if pending.empty:
        st.markdown("""
        <div class="no-trades">
            <div style="font-size:2.5rem;margin-bottom:10px;">✅</div>
            <div style="font-size:1rem;font-weight:700;color:white;">
                No pending trades to report
            </div>
            <div style="font-size:0.85rem;opacity:0.7;margin-top:6px;">
                All your trades have been reported or none are assigned yet.
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    labels = [
        f"{r['commodity']} · {r['origin']} → {r['destination']}"
        for _, r in pending.iterrows()
    ]
    sel_idx = st.selectbox(
        "Which trade did you complete?", range(len(labels)),
        format_func=lambda i: labels[i]
    )
    sel = pending.iloc[sel_idx]

    # Show prediction card
    st.markdown(f"""
    <div class="trade-card normal">
        <span class="trade-tag tag-blue">Prediction</span>
        <div class="trade-commodity">{sel['commodity']}</div>
        <div class="trade-route">📍 {sel['origin']} → {sel['destination']}</div>
        <div class="stats-grid">
            <div class="stat-box">
                <div class="stat-label">Predicted buy</div>
                <div class="stat-value buy">{naira(sel['buy_price'])}</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Predicted sell</div>
                <div class="stat-value sell">{naira(sel['sell_price'])}</div>
            </div>
        </div>
        <div class="profit-banner">
            <div class="profit-label">Predicted profit</div>
            <div class="profit-value">{naira(sel['expected_profit_ngn'])}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(
        '<p class="section-hdr">What Actually Happened?</p>',
        unsafe_allow_html=True
    )

    with st.form("agent_report"):
        actual_buy = st.number_input(
            "Actual buy price (₦/unit)",
            value=float(sel["buy_price"] or 0), step=100.0
        )
        actual_sell = st.number_input(
            "Actual sell price (₦/unit)",
            value=float(sel["sell_price"] or 0), step=100.0
        )
        actual_transport = st.number_input(
            "Transport cost (₦/unit)", value=0.0, step=100.0
        )
        actual_qty = st.number_input(
            "Units moved",
            value=float(sel["recommended_quantity"] or 0), step=1.0
        )
        notes = st.text_area(
            "What happened? (optional)",
            placeholder=(
                "e.g. Market was very busy, price went higher. "
                "Roads were bad near Lokoja..."
            )
        )

        if actual_sell > 0:
            preview = actual_qty*(actual_sell-actual_buy-actual_transport)
            diff    = preview - float(sel["expected_profit_ngn"] or 0)
            colour  = GREEN if diff >= 0 else RED
            symbol  = "▲" if diff >= 0 else "▼"
            st.markdown(
                f'<div class="preview-box">'
                f'<div class="preview-label">Your actual profit</div>'
                f'<div class="preview-value">{naira(preview)}</div>'
                f'<div class="preview-diff" style="color:{colour};">'
                f'{symbol} {naira(abs(diff))} vs prediction'
                f'</div></div>',
                unsafe_allow_html=True
            )

        submitted = st.form_submit_button("✅ Submit Report", type="primary")

        if submitted:
            actual_profit = actual_qty*(actual_sell-actual_buy-actual_transport)
            conn = get_connection()
            conn.execute("""
                INSERT INTO actual_outcomes (
                    recommendation_id, commodity_id, corridor_id,
                    actual_buy_price, actual_sell_price,
                    actual_transport_cost, actual_quantity,
                    actual_profit_ngn, trip_date,
                    outcome_notes, data_source, agent_id
                )
                SELECT ?, r.commodity_id, r.corridor_id,
                       ?, ?, ?, ?, ?, DATE('now'), ?, 'Agent App', ?
                FROM optimization_recommendations r WHERE r.id=?
            """, (
                int(sel["id"]), actual_buy, actual_sell,
                actual_transport, actual_qty,
                round(actual_profit,2), notes,
                agent_id, int(sel["id"])
            ))
            conn.execute(
                "UPDATE optimization_recommendations "
                "SET status='Completed' WHERE id=?",
                (int(sel["id"]),)
            )
            conn.commit()

            st.markdown(f"""
            <div class="submit-success">
                <div style="font-size:2.5rem;margin-bottom:10px;">🎉</div>
                <div style="font-size:1.2rem;font-weight:800;margin-bottom:6px;">
                    Report Submitted!
                </div>
                <div style="font-size:1rem;opacity:0.85;">
                    Your actual profit: <strong>{naira(actual_profit)}</strong>
                </div>
                <div style="font-size:0.82rem;opacity:0.65;margin-top:8px;">
                    This data will improve next week's recommendations.
                </div>
            </div>
            """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# PAGE 3: SUBMIT PRICE
# ══════════════════════════════════════════════════════════

elif page == "💬 Price":
    st.markdown(
        '<p class="section-hdr">Submit a Market Price</p>',
        unsafe_allow_html=True
    )

    comms = query("SELECT id, name FROM commodities ORDER BY name")
    mkts  = query("""
        SELECT m.id, m.name FROM markets m
        WHERE m.state_id=? AND m.is_active=1
    """, (sid,)) if sid else pd.DataFrame()

    with st.form("price_form"):
        sel_comm = st.selectbox("Commodity", comms["name"].tolist())
        comm_id  = int(comms[comms["name"]==sel_comm].iloc[0]["id"])

        if not mkts.empty:
            sel_mkt = st.selectbox("Market", mkts["name"].tolist())
            mkt_id  = int(mkts[mkts["name"]==sel_mkt].iloc[0]["id"])
        else:
            st.info(
                "No markets set up for your state yet. "
                "Your price will be recorded at state level."
            )
            sel_mkt = agent_state
            mkt_id  = None

        price = st.number_input(
            "Price (₦ per unit)", min_value=0.0, step=100.0
        )
        quantity = st.number_input(
            "Estimated quantity in market (units)",
            min_value=0.0, step=10.0,
            help="Rough estimate of how much is available today."
        )
        obs_date = st.date_input("Date observed", value=date.today())

        if st.form_submit_button("📤 Submit Price", type="primary"):
            if price <= 0:
                st.error("Enter a price greater than ₦0.")
            elif not sid:
                st.error("State not configured. Contact your supervisor.")
            else:
                conn = get_connection()
                conn.execute("""
                    INSERT INTO raw_submissions (
                        agent_id, state_id, market_id, commodity_id,
                        reported_price, quantity_available,
                        submission_date, source_channel
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Agent App')
                """, (
                    agent_id, sid, mkt_id, comm_id,
                    float(price),
                    float(quantity) if quantity > 0 else None,
                    str(obs_date)
                ))
                conn.commit()
                st.markdown(f"""
                <div class="submit-success">
                    <div style="font-size:2.5rem;margin-bottom:10px;">📤</div>
                    <div style="font-size:1.1rem;font-weight:800;margin-bottom:6px;">
                        Price Submitted!
                    </div>
                    <div style="font-size:0.9rem;opacity:0.85;">
                        {sel_comm} at {naira(price)}/unit<br>
                        {sel_mkt} · {obs_date}
                    </div>
                    <div style="font-size:0.78rem;opacity:0.6;margin-top:8px;">
                        Your data helps improve recommendations for everyone.
                    </div>
                </div>
                """, unsafe_allow_html=True)

    # Recent submissions
    st.markdown(
        '<p class="section-hdr">Your Recent Submissions</p>',
        unsafe_allow_html=True
    )
    recent = query("""
        SELECT rs.submission_date AS Date,
               c.name AS Commodity,
               rs.reported_price AS Price,
               m.name AS Market
        FROM raw_submissions rs
        JOIN commodities c ON rs.commodity_id=c.id
        LEFT JOIN markets m ON rs.market_id=m.id
        WHERE rs.agent_id=?
        ORDER BY rs.submission_date DESC LIMIT 10
    """, (agent_id,))

    if not recent.empty:
        recent["Price"] = recent["Price"].apply(naira)
        st.dataframe(recent, width='stretch', hide_index=True)
    else:
        st.markdown(
            '<div class="no-trades" style="padding:20px;">'
            '<div style="color:white;font-size:0.9rem;">'
            'No submissions yet. Use the form above.'
            '</div></div>',
            unsafe_allow_html=True
        )
