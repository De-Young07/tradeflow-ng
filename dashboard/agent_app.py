"""
TradeFlow NG — Agent Dashboard
Mobile-first. Professional. Fixed auth, logo, and conn.execute errors.
"""

import streamlit as st
import pandas as pd
from datetime import date, datetime
import sys, os

# ── Load DATABASE_URL BEFORE importing db_adapter ─────────
try:
    os.environ["DATABASE_URL"] = st.secrets["database"]["DATABASE_URL"]
except Exception:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from auth import require_agent_login, agent_logout
from db_adapter import query, execute, backend_name

# ── Logo paths ─────────────────────────────────────────────
_ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')
LOGO_FULL  = os.path.join(_ASSET_DIR, 'logo-full.png')
LOGO_ICON  = os.path.join(_ASSET_DIR, 'logo-icon.png')

# Fallback: try old names if new ones don't exist
if not os.path.exists(LOGO_FULL):
    LOGO_FULL = os.path.join(_ASSET_DIR, 'TradeFlow dark.jpg')
if not os.path.exists(LOGO_ICON):
    LOGO_ICON = os.path.join(_ASSET_DIR, 'TradeFlow logo.png')

# page_icon must be a valid path or emoji
_icon = LOGO_ICON if os.path.exists(LOGO_ICON) else "🌾"

st.set_page_config(
    page_title="TradeFlow NG — Agent",
    page_icon=_icon,
    layout="centered",
)

GREEN = "#1A6B3C"
LIME  = "#2ECC71"
AMBER = "#F39C12"
RED   = "#E74C3C"
DARK  = "#0D1F14"
GRAY  = "#6B7A70"

def naira(v):
    try:    return f"₦{float(v):,.0f}"
    except: return "₦—"

# ══════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {{
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}}

/* Background */
.stApp {{
    background: linear-gradient(160deg, #0a2e1a 0%, #0f3d22 50%, #1a5c35 100%);
    min-height: 100vh;
}}

/* ── Inputs always white and readable ── */
.stTextInput input, .stNumberInput input,
.stTextArea textarea, .stDateInput input {{
    background: #ffffff !important;
    color: #1A1A1A !important;
    border: 1.5px solid #C8DED0 !important;
    border-radius: 10px !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}}
.stTextInput input:focus, .stNumberInput input:focus,
.stTextArea textarea:focus {{
    border-color: {GREEN} !important;
    box-shadow: 0 0 0 3px rgba(26,107,60,0.12) !important;
}}
.stSelectbox > div > div {{
    background: #ffffff !important;
    color: #1A1A1A !important;
    border: 1.5px solid #C8DED0 !important;
    border-radius: 10px !important;
}}
/* Labels outside forms → white (on dark background) */
.stTextInput label, .stNumberInput label,
.stTextArea label, .stSelectbox label,
.stDateInput label {{
    color: rgba(255,255,255,0.85) !important;
    font-weight: 600 !important;
    font-size: 0.88rem !important;
}}

/* Labels INSIDE st.form → dark (on white form background) */
[data-testid="stForm"] .stTextInput label,
[data-testid="stForm"] .stNumberInput label,
[data-testid="stForm"] .stTextArea label,
[data-testid="stForm"] .stSelectbox label,
[data-testid="stForm"] .stDateInput label,
[data-testid="stForm"] label {{
    color: #1A1A1A !important;
    font-weight: 600 !important;
}}

/* Form container itself - white background */
[data-testid="stForm"] {{
    background: rgba(255,255,255,0.97) !important;
    border-radius: 18px !important;
    padding: 20px 24px !important;
    box-shadow: 0 4px 18px rgba(0,0,0,0.13) !important;
}}

/* Selectbox dropdown text always dark */
.stSelectbox [data-baseweb="select"] span,
.stSelectbox [data-baseweb="select"] div {{
    color: #1A1A1A !important;
}}

/* ── Logo container ── */
.logo-wrap {{
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 18px 0 8px;
}}
.logo-wrap img {{
    max-height: 48px;
    width: auto;
}}
.logo-text-fallback {{
    font-size: 1.4rem;
    font-weight: 800;
    color: #ffffff;
    letter-spacing: -0.02em;
}}
.logo-text-fallback span {{ color: rgba(255,255,255,0.55); font-weight:500; }}

/* ── Hero ── */
@keyframes slideDown {{
    from {{ opacity:0; transform:translateY(-16px); }}
    to   {{ opacity:1; transform:translateY(0); }}
}}
@keyframes fadeIn {{
    from {{ opacity:0; transform:translateY(10px); }}
    to   {{ opacity:1; transform:translateY(0); }}
}}
@keyframes shimmer {{
    0%   {{ background-position: -200% center; }}
    100% {{ background-position:  200% center; }}
}}
@keyframes cardIn {{
    from {{ opacity:0; transform:translateY(14px); }}
    to   {{ opacity:1; transform:translateY(0); }}
}}

.hero {{
    background: linear-gradient(135deg, #1A6B3C 0%, #0d4a27 100%);
    border-radius: 20px;
    padding: 24px 20px 20px;
    margin-bottom: 20px;
    text-align: center;
    color: white;
    animation: slideDown 0.5s ease-out;
    position: relative;
    overflow: hidden;
    box-shadow: 0 6px 28px rgba(0,0,0,0.3);
}}
.hero::before {{
    content: '';
    position: absolute;
    top: -50%; left: -50%;
    width: 200%; height: 200%;
    background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.05) 50%, transparent 100%);
    background-size: 200% 100%;
    animation: shimmer 3s infinite;
}}
.hero-icon  {{ font-size: 2.4rem; margin-bottom: 6px; }}
.hero-name  {{ font-size: 1.4rem; font-weight: 800; margin: 4px 0; letter-spacing: -0.02em; }}
.hero-sub   {{ font-size: 0.85rem; opacity: 0.75; margin-bottom: 10px; }}
.hero-badge {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(255,255,255,0.14);
    border: 1px solid rgba(255,255,255,0.22);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: 0.76rem;
    font-weight: 600;
}}
.hero-id {{
    display: inline-block;
    background: rgba(255,255,255,0.1);
    border: 1px dashed rgba(255,255,255,0.3);
    border-radius: 100px;
    padding: 2px 10px;
    font-size: 0.72rem;
    margin-top: 8px;
    font-family: 'DM Mono', monospace;
    letter-spacing: 0.06em;
}}

/* ── Nav tabs ── */
.stRadio > div {{
    display: flex;
    gap: 6px;
    background: rgba(255,255,255,0.08);
    border-radius: 14px;
    padding: 5px;
    backdrop-filter: blur(8px);
    margin-bottom: 16px;
}}
.stRadio label {{
    flex: 1;
    text-align: center;
    background: transparent;
    border-radius: 10px;
    padding: 10px 4px !important;
    cursor: pointer;
    color: rgba(255,255,255,0.65) !important;
    font-weight: 600;
    font-size: 0.82rem;
    transition: all 0.2s ease !important;
    border: none !important;
}}
.stRadio label:hover {{
    background: rgba(255,255,255,0.1) !important;
    color: white !important;
}}

/* ── Trade cards ── */
.trade-card {{
    background: rgba(255,255,255,0.97);
    border-radius: 18px;
    padding: 20px;
    margin-bottom: 14px;
    box-shadow: 0 4px 18px rgba(0,0,0,0.13);
    animation: cardIn 0.35s ease-out both;
    border: 1px solid rgba(255,255,255,0.4);
    transition: transform 0.2s, box-shadow 0.2s;
}}
.trade-card:hover {{ transform: translateY(-2px); box-shadow: 0 8px 28px rgba(0,0,0,0.18); }}
.trade-card.urgent {{ border-top: 5px solid {AMBER}; }}
.trade-card.normal {{ border-top: 5px solid {LIME}; }}

.trade-tag {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.68rem;
    font-weight: 700;
    margin-bottom: 8px;
    margin-right: 4px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}
.tag-green {{ background: #D4EDDA; color: #155724; }}
.tag-amber {{ background: #FFF3CD; color: #856404; }}
.tag-blue  {{ background: #D6EAF8; color: #1A5276; }}
.tag-red   {{ background: #FDECEA; color: #8B1A1A; }}

.trade-commodity {{
    font-size: 1.25rem;
    font-weight: 800;
    color: {DARK};
    margin-bottom: 2px;
}}
.trade-route {{
    font-size: 0.87rem;
    color: {GRAY};
    margin-bottom: 14px;
}}

.stats-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
    margin-bottom: 14px;
}}
.stat-box {{
    background: #F8FAF9;
    border-radius: 12px;
    padding: 11px;
    border: 1px solid #E4EEE8;
}}
.stat-label {{
    font-size: 0.63rem;
    color: #999;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 3px;
    font-weight: 600;
}}
.stat-value        {{ font-size: 1rem; font-weight: 700; color: {DARK}; }}
.stat-value.buy    {{ color: {RED}; }}
.stat-value.sell   {{ color: {GREEN}; }}
.stat-value.profit {{ color: {GREEN}; font-size: 1.1rem; }}

.profit-banner {{
    background: linear-gradient(135deg, {GREEN} 0%, #0d4a27 100%);
    border-radius: 14px;
    padding: 14px 16px;
    text-align: center;
    color: white;
    position: relative;
    overflow: hidden;
}}
.profit-banner::after {{
    content: '₦';
    position: absolute;
    right: -8px; top: -8px;
    font-size: 4.5rem;
    opacity: 0.07;
    font-weight: 900;
}}
.profit-label  {{ font-size: 0.7rem; opacity: 0.75; text-transform: uppercase; letter-spacing: 0.06em; }}
.profit-value  {{ font-size: 1.7rem; font-weight: 800; letter-spacing: -0.02em; }}
.profit-margin {{ font-size: 0.78rem; opacity: 0.7; margin-top: 2px; }}

.warn-strip {{
    background: #FFF8E6;
    border: 1px solid #FFD980;
    border-radius: 10px;
    padding: 8px 12px;
    font-size: 0.78rem;
    color: #7A5C00;
    margin-top: 8px;
    display: flex; gap: 6px; align-items: flex-start;
}}

/* ── Forms ── */
/* Remove duplicate - handled by the scoped rule above */

/* ── Headings on dark background → white ── */
h1, h2, h3, h4 {{
    color: #ffffff !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}}

/* Streamlit's native st.title / st.subheader elements */
[data-testid="stHeadingWithActionElements"] h1,
[data-testid="stHeadingWithActionElements"] h2,
[data-testid="stHeadingWithActionElements"] h3 {{
    color: #ffffff !important;
}}

/* Section header label */
.section-hdr {{
    color: rgba(255,255,255,0.45) !important;
}}

/* st.caption and st.info on dark background */
[data-testid="stCaptionContainer"] p {{
    color: rgba(255,255,255,0.55) !important;
}}

/* ── Buttons ── */
.stButton > button {{
    border-radius: 12px !important;
    font-weight: 700 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    transition: all 0.2s !important;
    border: none !important;
}}
.stButton > button[kind="primary"] {{
    background: linear-gradient(135deg, {GREEN} 0%, #0d4a27 100%) !important;
    color: white !important;
    padding: 13px 24px !important;
    font-size: 0.97rem !important;
    width: 100% !important;
    box-shadow: 0 4px 14px rgba(26,107,60,0.38) !important;
}}
.stButton > button[kind="primary"]:hover {{
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 20px rgba(26,107,60,0.5) !important;
}}
.stButton > button[kind="secondary"] {{
    background: rgba(255,255,255,0.1) !important;
    color: white !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    font-size: 0.82rem !important;
}}

/* ── Metrics ── */
[data-testid="metric-container"] {{
    background: rgba(255,255,255,0.96);
    border-radius: 14px;
    border: none;
    border-top: 3px solid {LIME};
    padding: 12px;
    box-shadow: 0 3px 12px rgba(0,0,0,0.1);
    animation: cardIn 0.4s ease-out;
}}

/* ── DataFrames ── */
.stDataFrame {{
    background: rgba(255,255,255,0.97);
    border-radius: 14px;
    overflow: hidden;
    box-shadow: 0 3px 12px rgba(0,0,0,0.1);
}}

/* ── Empty state ── */
.empty-state {{
    background: rgba(255,255,255,0.07);
    border-radius: 18px;
    padding: 44px 24px;
    text-align: center;
    color: rgba(255,255,255,0.75);
    border: 1px solid rgba(255,255,255,0.1);
}}
.empty-icon {{ font-size: 2.8rem; margin-bottom: 10px; }}

/* ── Section header ── */
.section-hdr {{
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: rgba(255,255,255,0.45);
    margin: 22px 0 10px;
}}

/* ── Success card ── */
.submit-ok {{
    background: linear-gradient(135deg, {GREEN}, #0d4a27);
    color: white; border-radius: 18px;
    padding: 28px; text-align: center;
    animation: cardIn 0.4s ease-out;
    box-shadow: 0 8px 24px rgba(26,107,60,0.4);
    margin-top: 12px;
}}

/* ── Preview box ── */
.preview-box {{
    background: linear-gradient(135deg, #E8F8F0, #D4EDDA);
    border: 1px solid #B8DECC;
    border-radius: 14px;
    padding: 14px;
    text-align: center;
    margin: 10px 0;
    animation: fadeIn 0.3s ease-out;
}}
.preview-label {{ font-size: 0.72rem; color: {GRAY}; font-weight: 600; text-transform: uppercase; }}
.preview-value {{ font-size: 1.55rem; font-weight: 800; color: {GREEN}; }}
.preview-diff  {{ font-size: 0.8rem; margin-top: 4px; }}

/* ── Login card ── */
.login-card {{
    background: rgba(255,255,255,0.97);
    border-radius: 22px;
    padding: 34px 26px;
    text-align: center;
    box-shadow: 0 12px 40px rgba(0,0,0,0.25);
    animation: slideDown 0.5s ease-out;
    margin-top: 16px;
}}

/* ── Retail note banner ── */
.retail-note {{
    background: rgba(255,255,255,0.09);
    border: 1px solid rgba(255,255,255,0.18);
    border-left: 4px solid {AMBER};
    border-radius: 10px;
    padding: 10px 14px;
    font-size: 0.78rem;
    color: rgba(255,255,255,0.8);
    margin-bottom: 14px;
    line-height: 1.55;
}}

#MainMenu, footer, header {{ visibility: hidden; }}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════

authenticated, agent_data = require_agent_login()
if not authenticated:
    st.stop()

# ── Unpack agent data ──────────────────────────────────────
agent_db_id   = agent_data.get("id")          # integer row id in agents table
agent_id_text = agent_data.get("agent_id")    # e.g. TFN-KW-001
agent_name    = agent_data.get("name", "Agent")
agent_state   = agent_data.get("state", "—")
sid           = agent_data.get("state_id")
market_name   = agent_data.get("market", "—")
market_id     = agent_data.get("market_id")
first_name    = agent_name.split()[0]

# ══════════════════════════════════════════════════════════
# HEADER — Logo + Hero
# ══════════════════════════════════════════════════════════

# Logo
if os.path.exists(LOGO_FULL):
    col_l, col_m, col_r = st.columns([1, 2, 1])
    with col_m:
        st.image(LOGO_FULL, use_container_width=True)
else:
    st.markdown(
        '<div class="logo-wrap">'
        '<span class="logo-text-fallback">TradeFlow <span>NG</span></span>'
        '</div>',
        unsafe_allow_html=True
    )

# Hero
st.markdown(f"""
<div class="hero">
    <div class="hero-icon">👋</div>
    <div class="hero-name">Hello, {first_name}!</div>
    <div class="hero-sub">{market_name} · {agent_state}</div>
    <div class="hero-badge">
        📅 {date.today().strftime('%d %b %Y')}
        &nbsp;|&nbsp; Week {date.today().isocalendar()[1]}
    </div>
    <div class="hero-id">{agent_id_text or '—'}</div>
</div>
""", unsafe_allow_html=True)

# Navigation
page = st.radio(
    "", ["📋 My Trades", "✅ Report", "💬 Submit Price"],
    horizontal=True, label_visibility="collapsed"
)

# Logout
col_out, _ = st.columns([1, 3])
with col_out:
    if st.button("🚪 Logout", key="logout_btn"):
        agent_logout()
        st.rerun()


# ══════════════════════════════════════════════════════════
# PAGE 1: MY TRADES
# ══════════════════════════════════════════════════════════

if page == "📋 My Trades":

    recs_all = query("""
        SELECT r.id,
               r.sell_price - r.buy_price - r.transport_cost AS profit_per_unit,
               co.name                 AS commodity,
               s_orig.name             AS origin,
               s_dest.name             AS destination,
               corr.distance_km,
               corr.road_quality,
               c2.perishability_class,
               r.recommended_quantity,
               r.buy_price,
               r.sell_price,
               r.transport_cost,
               r.expected_profit_ngn,
               r.profit_margin_pct,
               r.is_shock_flagged,
               r.is_backhaul,
               r.missing_cost_flag,
               r.shock_reason
        FROM   optimization_recommendations r
        JOIN   commodities co   ON r.commodity_id = co.id
        JOIN   commodities c2   ON r.commodity_id = c2.id
        LEFT JOIN corridors corr ON r.corridor_id  = corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id = s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id   = s_dest.id
        WHERE  r.run_id = (SELECT MAX(id) FROM optimization_runs)
          AND  r.status = 'Pending'
          AND  (corr.origin_state_id = ? OR corr.dest_state_id = ?)
        ORDER BY r.expected_profit_ngn DESC
    """, (sid, sid)) if sid else pd.DataFrame()

    if not recs_all.empty:
        # Safely coerce booleans
        for col in ["is_shock_flagged", "is_backhaul", "missing_cost_flag"]:
            if col in recs_all.columns:
                recs_all[col] = recs_all[col].apply(
                    lambda x: bool(x) if x is not None else False
                )

        c1, c2, c3 = st.columns(3)
        c1.metric("🚚 Routes",     len(recs_all))
        c2.metric("💰 Total Pot.", naira(recs_all["expected_profit_ngn"].sum()))
        c3.metric("⚠️ High-Risk",  int(recs_all["is_shock_flagged"].sum()))

    st.markdown('<p class="section-hdr">This Week\'s Trade Assignments</p>',
                unsafe_allow_html=True)

    # Retail note
    st.markdown("""
    <div class="retail-note">
        💡 <strong>Note on prices:</strong> Buy prices shown are wholesale estimates.
        Sell prices are based on retail market data from your fellow agents.
        Your actual wholesale buy price will typically be 10–25% below the retail price shown.
        Use the margin as a guide — your real profit may be higher.
    </div>
    """, unsafe_allow_html=True)

    if recs_all.empty:
        st.markdown("""
        <div class="empty-state">
            <div class="empty-icon">📭</div>
            <div style="font-size:1rem;font-weight:700;margin-bottom:6px;">No trades assigned yet</div>
            <div style="font-size:0.85rem;opacity:0.7;">
                The optimizer hasn't run this week or no routes cover your state.<br>
                Check back later or contact your supervisor.
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        for i, (_, row) in enumerate(recs_all.iterrows()):
            is_risk  = bool(row.get("is_shock_flagged", False))
            no_cost  = float(row.get("transport_cost", 0) or 0) == 0
            is_back  = bool(row.get("is_backhaul", False))
            perishab = str(row.get("perishability_class", ""))
            profit   = float(row.get("expected_profit_ngn", 0) or 0)
            margin   = float(row.get("profit_margin_pct",  0) or 0)
            dist     = row.get("distance_km")
            road     = str(row.get("road_quality") or "")

            tags  = []
            if not is_risk and not no_cost:
                tags.append('<span class="trade-tag tag-green">Verified</span>')
            if is_risk:
                tags.append('<span class="trade-tag tag-amber">⚠ High Risk</span>')
            if is_back:
                tags.append('<span class="trade-tag tag-blue">↩ Backhaul</span>')
            if perishab == "Perishable":
                tags.append('<span class="trade-tag tag-red">⚡ Perishable</span>')

            warns = []
            if is_risk:
                warns.append(
                    '<div class="warn-strip">⚠ Forecast uncertain this week — '
                    'verify market price before committing capital.</div>'
                )
            if no_cost:
                warns.append(
                    '<div class="warn-strip">⚠ Transport cost not confirmed — '
                    'actual profit may differ.</div>'
                )

            route_note = f" · {dist:.0f}km, {road} road" if dist else ""

            card = (
                f'<div class="trade-card {"urgent" if is_risk else "normal"}" '
                f'style="animation-delay:{i*0.07}s;">'
                + "".join(tags)
                + f'<div class="trade-commodity">🌾 {row["commodity"]}</div>'
                + f'<div class="trade-route">📍 {row["origin"]} → {row["destination"]}{route_note}</div>'
                + '<div class="stats-grid">'
                + f'<div class="stat-box"><div class="stat-label">Buy at origin</div>'
                + f'<div class="stat-value buy">{naira(row["buy_price"])}</div></div>'
                + f'<div class="stat-box"><div class="stat-label">Sell at dest.</div>'
                + f'<div class="stat-value sell">{naira(row["sell_price"])}</div></div>'
                + f'<div class="stat-box"><div class="stat-label">Quantity</div>'
                + f'<div class="stat-value">{float(row["recommended_quantity"]):.0f} units</div></div>'
                + f'<div class="stat-box"><div class="stat-label">Transport/unit</div>'
                + f'<div class="stat-value">{naira(row["transport_cost"])}</div></div>'
                + '</div>'
                + '<div class="profit-banner">'
                + '<div class="profit-label">Expected Total Profit</div>'
                + f'<div class="profit-value">{naira(profit)}</div>'
                + f'<div class="profit-margin">{margin:.1f}% margin</div>'
                + '</div>'
                + "".join(warns)
                + '</div>'
            )
            st.markdown(card, unsafe_allow_html=True)

    # Local prices
    st.markdown(
        '<p class="section-hdr">Latest Prices in Your State</p>',
        unsafe_allow_html=True
    )
    local = query("""
        SELECT c.name          AS commodity,
               cp.price_per_unit AS price,
               cp.price_date   AS date
        FROM   cleaned_prices cp
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  cp.state_id = ?
          AND  cp.price_date = (
              SELECT MAX(p2.price_date)
              FROM   cleaned_prices p2
              WHERE  p2.state_id     = cp.state_id
                AND  p2.commodity_id = cp.commodity_id
              )
        ORDER BY c.name
    """, (sid,)) if sid else pd.DataFrame()

    if not local.empty:
        local["price"] = local["price"].apply(naira)
        st.dataframe(local, use_container_width=True, hide_index=True)
    else:
        st.markdown(
            '<div class="empty-state" style="padding:22px;">'
            '<div style="font-size:0.88rem;">No price data for your state yet.</div>'
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
               r.recommended_quantity,
               r.buy_price, r.sell_price, r.expected_profit_ngn
        FROM   optimization_recommendations r
        JOIN   commodities co ON r.commodity_id = co.id
        LEFT JOIN corridors corr ON r.corridor_id = corr.id
        LEFT JOIN states s_orig  ON corr.origin_state_id = s_orig.id
        LEFT JOIN states s_dest  ON corr.dest_state_id   = s_dest.id
        WHERE  r.run_id = (SELECT MAX(id) FROM optimization_runs)
          AND  r.status = 'Pending'
          AND  (corr.origin_state_id = ? OR corr.dest_state_id = ?)
        ORDER BY r.id DESC
    """, (sid, sid)) if sid else pd.DataFrame()

    if pending.empty:
        st.markdown("""
        <div class="empty-state">
            <div class="empty-icon">✅</div>
            <div style="font-size:1rem;font-weight:700;margin-bottom:6px;">All caught up!</div>
            <div style="font-size:0.85rem;opacity:0.7;">
                No pending trades to report right now.
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    labels = [
        f"{r['commodity']} · {r['origin']} → {r['destination']}"
        for _, r in pending.iterrows()
    ]
    sel_idx = st.selectbox("Which trade did you complete?",
                           range(len(labels)),
                           format_func=lambda i: labels[i])
    sel = pending.iloc[sel_idx]

    st.markdown(f"""
    <div class="trade-card normal">
        <span class="trade-tag tag-blue">System Prediction</span>
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
            "Your actual buy price (₦/unit)",
            value=float(sel["buy_price"] or 0), step=100.0,
            help="What you actually paid per unit at the origin market"
        )
        actual_sell = st.number_input(
            "Your actual sell price (₦/unit)",
            value=float(sel["sell_price"] or 0), step=100.0,
            help="What you actually received per unit at the destination"
        )
        actual_transport = st.number_input(
            "Actual transport cost (₦/unit)",
            value=0.0, step=100.0
        )
        actual_qty = st.number_input(
            "Units moved",
            value=float(sel["recommended_quantity"] or 1), step=1.0
        )
        trade_date = st.date_input("Date of trade", value=date.today())
        notes = st.text_area(
            "Notes (optional)",
            placeholder="e.g. Market was busy, price went higher than expected..."
        )

        if actual_sell > 0:
            preview  = actual_qty * (actual_sell - actual_buy - actual_transport)
            diff     = preview - float(sel["expected_profit_ngn"] or 0)
            colour   = GREEN if diff >= 0 else RED
            symbol   = "▲" if diff >= 0 else "▼"
            st.markdown(
                f'<div class="preview-box">'
                f'<div class="preview-label">Your actual profit</div>'
                f'<div class="preview-value">{naira(preview)}</div>'
                f'<div class="preview-diff" style="color:{colour};">'
                f'{symbol} {naira(abs(diff))} vs prediction</div>'
                f'</div>',
                unsafe_allow_html=True
            )

        submitted = st.form_submit_button("✅ Submit Report", type="primary")

        if submitted:
            actual_profit = actual_qty * (actual_sell - actual_buy - actual_transport)
            try:
                # Use db_adapter.execute — routes through db_adapter
                execute("""
                    INSERT INTO actual_outcomes (
                        recommendation_id, commodity_id, corridor_id,
                        state_id, agent_id,
                        actual_buy_price, actual_sell_price,
                        actual_transport_cost, actual_quantity,
                        actual_profit_ngn, trade_date,
                        outcome_notes, data_source
                    )
                    SELECT ?, r.commodity_id, r.corridor_id,
                           ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, 'Agent App'
                    FROM   optimization_recommendations r
                    WHERE  r.id = ?
                """, (
                    int(sel["id"]),
                    sid, agent_db_id,
                    actual_buy, actual_sell,
                    actual_transport, actual_qty,
                    round(actual_profit, 2),
                    str(trade_date),
                    notes,
                    int(sel["id"]),
                ))
                execute(
                    "UPDATE optimization_recommendations "
                    "SET status = ? WHERE id = ?",
                    ("Completed", int(sel["id"]))
                )
                st.markdown(f"""
                <div class="submit-ok">
                    <div style="font-size:2.2rem;margin-bottom:8px;">🎉</div>
                    <div style="font-size:1.15rem;font-weight:800;margin-bottom:6px;">
                        Report Submitted!
                    </div>
                    <div style="font-size:0.95rem;opacity:0.85;">
                        Your actual profit: <strong>{naira(actual_profit)}</strong>
                    </div>
                    <div style="font-size:0.78rem;opacity:0.6;margin-top:8px;">
                        This data improves next week's recommendations for everyone.
                    </div>
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Could not save report: {e}")


# ══════════════════════════════════════════════════════════
# PAGE 3: SUBMIT PRICE
# ══════════════════════════════════════════════════════════

elif page == "💬 Submit Price":
    st.markdown(
        '<p class="section-hdr">Submit a Market Price</p>',
        unsafe_allow_html=True
    )

    st.markdown("""
    <div class="retail-note">
        📝 Submit the <strong>retail price</strong> you observe in the market —
        the price a buyer would pay for one bag or crate today.
        Do not estimate. Only report what you personally see or confirm with a seller.
    </div>
    """, unsafe_allow_html=True)

    comms = query("SELECT id, name FROM commodities ORDER BY name")

    # Load markets for this agent's state + their own assigned market
    mkts = query("""
        SELECT m.id, m.name
        FROM   markets m
        WHERE  m.state_id = ?
          AND  m.is_active = 1
        ORDER  BY m.name
    """, (sid,)) if sid else pd.DataFrame()

    with st.form("price_form"):
        if comms.empty:
            st.warning("No commodities set up. Contact your supervisor.")
            st.stop()

        sel_comm = st.selectbox("Commodity", comms["name"].tolist())
        comm_id  = int(comms[comms["name"] == sel_comm].iloc[0]["id"])

        if not mkts.empty:
            # Pre-select agent's own market if it's in the list
            default_mkt_idx = 0
            if market_name and market_name != "—":
                names = mkts["name"].tolist()
                for idx, n in enumerate(names):
                    if market_name.lower() in n.lower():
                        default_mkt_idx = idx
                        break
            sel_mkt = st.selectbox("Market", mkts["name"].tolist(),
                                   index=default_mkt_idx)
            sel_mkt_id = int(mkts[mkts["name"] == sel_mkt].iloc[0]["id"])
        else:
            st.info(
                "No markets set up for your state yet. "
                "Your price will be recorded at state level only."
            )
            sel_mkt    = agent_state
            sel_mkt_id = market_id  # agent's own market_id if set

        price = st.number_input(
            "Retail price (₦ per unit)",
            min_value=0.0, step=100.0,
            help="The price a buyer pays per bag or crate today at this market"
        )
        quantity = st.number_input(
            "Estimated quantity available (units)",
            min_value=0.0, step=5.0,
            help="Roughly how many bags/crates are available today"
        )
        quality = st.selectbox(
            "Quality grade",
            ["Grade A (Excellent)", "Grade B (Good)", "Mixed", "Grade C (Fair)"]
        )
        availability = st.selectbox(
            "Availability level",
            ["Abundant", "Adequate", "Limited", "Scarce"]
        )
        obs_date = st.date_input("Date observed", value=date.today())
        notes    = st.text_area(
            "Market notes (optional)",
            placeholder="e.g. Road was bad near Lokoja, market was quiet, "
                        "price dropped due to surplus..."
        )

        submitted = st.form_submit_button("📤 Submit Price", type="primary")

        if submitted:
            if price <= 0:
                st.error("Please enter a price greater than ₦0.")
            elif not sid:
                st.error("Your state is not configured. Contact your supervisor.")
            else:
                try:
                    execute("""
                        INSERT INTO raw_submissions (
                            agent_id, state_id, market_id, commodity_id,
                            reported_price, quantity_available,
                            quality_grade, market_activity,
                            submission_date, source_channel, notes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Agent App', ?)
                    """, (
                        agent_db_id,
                        sid,
                        sel_mkt_id,
                        comm_id,
                        float(price),
                        float(quantity) if quantity > 0 else None,
                        quality,
                        availability,
                        str(obs_date),
                        notes or None,
                    ))
                    st.markdown(f"""
                    <div class="submit-ok">
                        <div style="font-size:2.2rem;margin-bottom:8px;">📤</div>
                        <div style="font-size:1.1rem;font-weight:800;margin-bottom:6px;">
                            Price Submitted!
                        </div>
                        <div style="font-size:0.9rem;opacity:0.85;">
                            {sel_comm} · {naira(price)}/unit<br>
                            {sel_mkt} · {obs_date}
                        </div>
                        <div style="font-size:0.75rem;opacity:0.6;margin-top:8px;">
                            Your data is now in the TradeFlow NG system.
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Could not save submission: {e}")

    # Recent submissions
    st.markdown(
        '<p class="section-hdr">Your Recent Submissions</p>',
        unsafe_allow_html=True
    )
    recent = query("""
        SELECT rs.submission_date AS date,
               c.name             AS commodity,
               rs.reported_price  AS price,
               m.name             AS market,
               rs.quality_grade   AS quality
        FROM   raw_submissions rs
        JOIN   commodities c ON rs.commodity_id = c.id
        LEFT JOIN markets  m ON rs.market_id    = m.id
        WHERE  rs.agent_id = ?
        ORDER  BY rs.submission_date DESC
        LIMIT  15
    """, (agent_db_id,))

    if not recent.empty:
        recent["price"] = recent["price"].apply(naira)
        st.dataframe(recent, use_container_width=True, hide_index=True)
    else:
        st.markdown(
            '<div class="empty-state" style="padding:20px;">'
            '<div style="font-size:0.88rem;">No submissions yet. '
            'Use the form above to submit your first price.</div>'
            '</div>',
            unsafe_allow_html=True
        )
