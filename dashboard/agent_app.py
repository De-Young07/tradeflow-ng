"""
TradeFlow NG — Agent Dashboard v2
Mobile-first, interactive, branded design.
"""

import streamlit as st
import pandas as pd
from datetime import date, datetime
import sys, os

# ══════════════════════════════════════════════════════════
# ENVIRONMENT & IMPORTS
# ══════════════════════════════════════════════════════════
# Load DATABASE_URL from Streamlit secrets into environment
try:
    os.environ["DATABASE_URL"] = st.secrets["database"]["DATABASE_URL"]
except (KeyError, FileNotFoundError):
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import auth and db functions
from auth import require_agent_login, agent_logout
from db_adapter import query, get_connection

# ══════════════════════════════════════════════════════════
# PAGE CONFIGURATION & BRANDING
# ══════════════════════════════════════════════════════════
st.set_page_config(
    page_title="TradeFlow NG | Field Agent",
    page_icon="🌾",
    layout="centered",
)

# TradeFlow NG Brand Colors
GREEN  = "#1A6B3C"
DARK   = "#1A1A2E"
LIGHT  = "#E8F8F0"

st.markdown(f"""
    <style>
    .brand-header {{
        background-color: {LIGHT};
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid {GREEN};
        margin-bottom: 20px;
        display: flex;
        align-items: center;
    }}
    .agent-badge {{
        background-color: {GREEN};
        color: white;
        padding: 4px 10px;
        border-radius: 15px;
        font-size: 0.8rem;
        font-weight: bold;
    }}
    .welcome-text {{
        font-size: 1.2rem;
        color: {DARK};
        margin-top: 5px;
    }}
    </style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════
# AUTHENTICATION
# ══════════════════════════════════════════════════════════
is_auth, agent_data = require_agent_login()

if not is_auth:
    st.stop()

# Safely extract variables from session_state (Fixes the KeyError)
agent_name  = st.session_state.get("agent_name", "Field Agent")
agent_id    = st.session_state.get("agent_id", 0)
agent_state = st.session_state.get("agent_state", "—")
state_id    = st.session_state.get("agent_state_id", 0)
market_name = st.session_state.get("agent_market", "Assigned Market")

# ══════════════════════════════════════════════════════════
# BRANDED UI HEADER
# ══════════════════════════════════════════════════════════
top_col1, top_col2 = st.columns([3, 1])
with top_col1:
    try:
        st.image("TradeFlow logo.png", width=180) 
    except FileNotFoundError:
        st.markdown(f"**TradeFlow** NG", unsafe_allow_html=True)

with top_col2:
    if st.button("🚪 Logout", use_container_width=True):
        agent_logout()
        st.rerun()

try:
    st.image("TradeFlow profile.jpg", use_column_width=True, caption="Real-time Market Intelligence")
except FileNotFoundError:
    pass

st.markdown(f"""
    <div class="brand-header">
        <div>
            <span class="agent-badge">📍 {agent_state} State | {market_name}</span>
            <div class="welcome-text">Welcome back, <strong>{agent_name}</strong></div>
        </div>
    </div>
""", unsafe_allow_html=True)

st.divider()

# ══════════════════════════════════════════════════════════
# DATA SUBMISSION FORM
# ══════════════════════════════════════════════════════════
st.markdown("### 📝 Submit Today's Prices")

# Fetch commodities from the database to populate the dropdown
try:
    commodities_df = query("SELECT id, name FROM commodities ORDER BY name")
    commodity_options = commodities_df['name'].tolist()
except Exception as e:
    st.error(f"Could not load commodities: {e}")
    st.stop()

with st.form("price_submission_form", clear_on_submit=True):
    col1, col2 = st.columns(2)
    
    with col1:
        selected_commodity = st.selectbox("Commodity", commodity_options)
        unit = st.selectbox("Unit of Measure", ["kg", "bag", "basket", "tuber"])
        
    with col2:
        price_per_unit = st.number_input("Current Market Price (₦)", min_value=0.0, step=100.0)
        est_quantity = st.number_input("Estimated Supply Available", min_value=0.0, step=10.0)
        
    notes = st.text_area("Market Notes / Weather Conditions (Optional)", placeholder="e.g., Heavy rain today, fewer trucks arrived...")
    
    submit = st.form_submit_button("✅ Submit Price Report", type="primary", use_container_width=True)

# ══════════════════════════════════════════════════════════
# DATABASE INSERTION LOGIC (Using strict PostgreSQL rules)
# ══════════════════════════════════════════════════════════
if submit:
    if price_per_unit <= 0:
        st.warning("⚠️ Price must be greater than zero.")
    else:
        with st.spinner("Saving data..."):
            try:
                # Find the ID of the selected commodity
                commodity_id = int(commodities_df.loc[commodities_df['name'] == selected_commodity, 'id'].values[0])
                
                # Connect to PostgreSQL via psycopg2
                conn = get_connection()
                cur = conn.cursor()
                
                # PostgreSQL safe query (using %s instead of ?)
                cur.execute("""
                    INSERT INTO raw_submissions (
                        agent_id, state_id, commodity_id, 
                        price_per_unit, unit, available_quantity, 
                        notes, submission_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    agent_id, 
                    state_id, 
                    commodity_id, 
                    price_per_unit, 
                    unit, 
                    est_quantity, 
                    notes, 
                    date.today()
                ))
                
                conn.commit()
                cur.close()
                conn.close()
                
                st.success(f"✅ Successfully logged {selected_commodity} at ₦{price_per_unit:,.0f}/{unit}!")
                st.balloons()
                
            except Exception as e:
                st.error(f"❌ Failed to submit data: {e}")
