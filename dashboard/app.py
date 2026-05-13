# TradeFlow NG — Main Dashboard
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Load DATABASE_URL from Streamlit secrets into environment
try:
    os.environ["DATABASE_URL"] = st.secrets["database"]["DATABASE_URL"]
except (KeyError, FileNotFoundError):
    pass

from db_adapter import query, execute, backend_name
from database import init_database, test_connection

st.set_page_config(page_title="TradeFlow NG", page_icon="📊", layout="wide")

# Initialize database on startup
if 'db_initialized' not in st.session_state:
    init_database()
    test_connection()
    st.session_state.db_initialized = True

GREEN = "#1A6B3C"
LIME = "#2ECC71"
AMBER = "#F39C12"
RED = "#E74C3C"
DARK = "#1A1A2E"
GRAY = "#7F8C8D"

def naira(v):
    try:
        return f"₦{float(v):,.0f}"
    except:
        return "₦—"

# ══════════════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════���══════════════════════════

page = st.sidebar.radio(
    "Navigate",
    ["📊 Overview", "💰 Optimization", "📈 Forecasting", "⚙️ Settings"]
)

if page == "📊 Overview":
    st.title("📊 Overview")
    st.caption("Bird's-eye view of TradeFlow NG — profit potential, price trends, and system health")

    # ── KPIs ──────────────────────────────────────────────────────────────────────
    try:
        latest_run = query("SELECT * FROM optimization_runs ORDER BY run_id DESC LIMIT 1")
        recs = query("""
            SELECT COUNT(*) as count FROM optimization_recommendations 
            WHERE status='Pending'
        """)
        
        n_prices_week = query("""
            SELECT COUNT(*) as count FROM cleaned_prices 
            WHERE price_date >= (CURRENT_DATE - INTERVAL '7 days')
        """)
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Last Optimization", latest_run["run_id"].iloc[0] if not latest_run.empty else "—")
        c2.metric("Pending Trades", int(recs["count"].iloc[0]) if not recs.empty else 0)
        c3.metric("Prices This Week", int(n_prices_week["count"].iloc[0]) if not n_prices_week.empty else 0)
        c4.metric("Database", backend_name())
    except Exception as e:
        st.error(f"KPI Error: {e}")

    # ── Price Trend Chart ──────────────────────────────────────────────────────────
    st.subheader("📈 Price Trends")
    comms = query("SELECT DISTINCT commodity_id, name FROM cleaned_prices JOIN commodities ON cleaned_prices.commodity_id = commodities.id ORDER BY name")
    
    if not comms.empty:
        sel_comm = st.selectbox("Select Commodity", comms["name"].unique())
        
        try:
            trend = query("""
                SELECT 
                    cp.price_date as date,
                    s.name as state,
                    cp.price_per_unit as price
                FROM cleaned_prices cp
                JOIN states s ON cp.state_id = s.id
                WHERE cp.commodity_id = (SELECT id FROM commodities WHERE name = %s)
                ORDER BY cp.price_date DESC
                LIMIT 100
            """, (sel_comm,))
            
            if not trend.empty:
                # Ensure column names are lowercase for consistency
                trend.columns = [col.lower() for col in trend.columns]
                fig = px.line(
                    trend, x="date", y="price", color="state",
                    title=f"{sel_comm} — Price per Unit (₦) by State",
                    labels={"price": "Price (₦/unit)"},
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"No price data for {sel_comm}")
        except Exception as e:
            st.error(f"Chart Error: {e}")
    else:
        st.info("No price data available")

elif page == "💰 Optimization":
    st.title("💰 Optimization")
    st.caption("View and manage trade recommendations")
    
    try:
        recs = query("""
            SELECT 
                r.id, 
                c.name as commodity,
                s_o.name as origin,
                s_d.name as destination,
                r.expected_profit_ngn as profit,
                r.profit_margin_pct as margin,
                r.status
            FROM optimization_recommendations r
            JOIN commodities c ON r.commodity_id = c.id
            LEFT JOIN corridors corr ON r.corridor_id = corr.id
            LEFT JOIN states s_o ON corr.origin_state_id = s_o.id
            LEFT JOIN states s_d ON corr.dest_state_id = s_d.id
            ORDER BY r.expected_profit_ngn DESC
            LIMIT 20
        """)
        
        if not recs.empty:
            # Normalize column names to lowercase
            recs.columns = [col.lower() for col in recs.columns]
            st.dataframe(recs, use_container_width=True)
        else:
            st.info("No optimization results yet")
    except Exception as e:
        st.error(f"Optimization Error: {e}")

elif page == "📈 Forecasting":
    st.title("📈 Forecasting")
    st.caption("Prophet forecasts for prices")
    
    try:
        forecasts = query("""
            SELECT 
                f.forecast_date as date,
                s.name as state,
                c.name as commodity,
                f.predicted_price as price,
                f.is_shock_flagged as shock
            FROM forecasts f
            JOIN states s ON f.state_id = s.id
            JOIN commodities c ON f.commodity_id = c.id
            ORDER BY f.forecast_date DESC
            LIMIT 50
        """)
        
        if not forecasts.empty:
            forecasts.columns = [col.lower() for col in forecasts.columns]
            st.dataframe(forecasts, use_container_width=True)
        else:
            st.info("No forecasts available")
    except Exception as e:
        st.error(f"Forecasting Error: {e}")

elif page == "⚙️ Settings":
    st.title("⚙️ Settings")
    st.caption("System configuration and utilities")
    
    st.subheader("Database Status")
    if st.button("Test Connection"):
        try:
            result = query("SELECT COUNT(*) as count FROM states")
            st.success(f"✅ Connected to {backend_name()}")
            st.info(f"States: {int(result['count'].iloc[0]) if not result.empty else 0}")
        except Exception as e:
            st.error(f"Connection failed: {e}")
    
    st.subheader("Data Health")
    try:
        tables_status = {
            "states": "SELECT COUNT(*) as count FROM states",
            "markets": "SELECT COUNT(*) as count FROM markets",
            "commodities": "SELECT COUNT(*) as count FROM commodities",
            "cleaned_prices": "SELECT COUNT(*) as count FROM cleaned_prices",
            "forecasts": "SELECT COUNT(*) as count FROM forecasts",
            "optimization_runs": "SELECT COUNT(*) as count FROM optimization_runs",
        }
        
        for table_name, sql in tables_status.items():
            try:
                result = query(sql)
                count = int(result['count'].iloc[0]) if not result.empty else 0
                st.write(f"✅ {table_name}: {count} records")
            except Exception as e:
                st.write(f"❌ {table_name}: {str(e)[:50]}")
    except Exception as e:
        st.error(f"Health check error: {e}")
