"""
TradeFlow NG — Authentication Module
Handles login for both admin and agent dashboards.

Admin:  username + password (from st.secrets)
Agents: phone number (must exist in agents table)

Usage:
    from auth import require_admin_login, require_agent_login
"""

import streamlit as st
import sqlite3
import pandas as pd
import os

# ── DB path ────────────────────────────────────────────────
DB_PATH = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"

def _get_db_path():
    """Get DB path — local SQLite or from environment."""
    database_url = os.environ.get("DATABASE_URL", "sqlite")
    if database_url.startswith("postgresql") or database_url.startswith("postgres"):
        return None  # Signal to use PostgreSQL
    return DB_PATH


def _query(sql, params=()):
    """Simple query helper."""
    db = _get_db_path()
    if db:
        conn = sqlite3.connect(db, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        return df
    else:
        # PostgreSQL path
        try:
            import psycopg2
            database_url = os.environ.get("DATABASE_URL")
            conn = psycopg2.connect(database_url)
            sql_pg = sql.replace("?", "%s")
            df = pd.read_sql(sql_pg, conn, params=params if params else None)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            return pd.DataFrame()


# ══════════════════════════════════════════════════════════
# ADMIN LOGIN
# ══════════════════════════════════════════════════════════

def require_admin_login():
    """
    Show admin login screen if not authenticated.
    Returns True if authenticated, False otherwise.
    Call at the top of app.py before any content.

    Credentials stored in .streamlit/secrets.toml:
        [auth]
        admin_username = "admin"
        admin_password = "your_password"
    """
    # Check if already logged in
    if st.session_state.get("admin_authenticated"):
        # Show logout in sidebar
        with st.sidebar:
            st.divider()
            st.caption(f"Logged in as **{st.session_state.get('admin_user', 'Admin')}**")
            if st.button("🚪 Logout", key="admin_logout"):
                st.session_state.admin_authenticated = False
                st.session_state.admin_user = None
                st.rerun()
        return True

    # Show login page
    st.markdown("""
    <style>
    .login-container {
        max-width: 420px;
        margin: 80px auto;
        padding: 40px;
        background: white;
        border-radius: 16px;
        border: 1px solid #D5E8DC;
        box-shadow: 0 4px 24px rgba(0,0,0,0.08);
    }
    </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 🌾 TradeFlow NG")
        st.markdown("### Admin Login")
        st.caption("Internal Control Dashboard")
        st.divider()

        username = st.text_input("Username", placeholder="admin")
        password = st.text_input("Password", type="password",
                                 placeholder="Enter your password")

        if st.button("Login →", type="primary", use_container_width=True):
            # Get credentials from secrets
            try:
                valid_user = st.secrets["auth"]["admin_username"]
                valid_pass = st.secrets["auth"]["admin_password"]
            except (KeyError, FileNotFoundError):
                # Fallback for local development without secrets file
                valid_user = os.environ.get("ADMIN_USERNAME", "admin")
                valid_pass = os.environ.get("ADMIN_PASSWORD", "tradeflow2026")

            if username == valid_user and password == valid_pass:
                st.session_state.admin_authenticated = True
                st.session_state.admin_user = username
                st.rerun()
            else:
                st.error("❌ Incorrect username or password.")

        st.divider()
        st.caption("Contact your system administrator if you've forgotten your credentials.")

    return False


# ══════════════════════════════════════════════════════════
# AGENT LOGIN
# ══════════════════════════════════════════════════════════

def require_agent_login():
    """
    Show agent login screen if not authenticated.
    Agents log in with their registered phone number.
    Returns (True, agent_data) if authenticated, (False, None) otherwise.
    """
    if st.session_state.get("agent_authenticated"):
        return True, {
            "agent_id":       st.session_state.agent_id,
            "agent_name":     st.session_state.agent_name,
            "agent_state":    st.session_state.agent_state,
            "agent_state_id": st.session_state.agent_state_id,
        }

    # Agents don't have a sidebar — show centered login
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 🌾 TradeFlow NG")
        st.markdown("### Agent Login")
        st.caption("Enter your registered phone number to continue.")
        st.divider()

        phone = st.text_input(
            "Phone number",
            placeholder="08012345678",
            help="The number you registered with your supervisor."
        )

        if st.button("Login →", type="primary", use_container_width=True):
            if not phone.strip():
                st.warning("Please enter your phone number.")
            else:
                agent = _query(
                    "SELECT * FROM agents WHERE phone = ? AND is_active = 1",
                    (phone.strip(),)
                )

                if not agent.empty:
                    a = agent.iloc[0]
                    state_row = _query(
                        "SELECT id, name FROM states WHERE id = ?",
                        (int(a["state_id"]),)
                    )

                    st.session_state.agent_authenticated = True
                    st.session_state.agent_id            = int(a["id"])
                    st.session_state.agent_name          = a["full_name"]
                    st.session_state.agent_state         = (
                        state_row.iloc[0]["name"] if not state_row.empty else "—"
                    )
                    st.session_state.agent_state_id      = (
                        int(state_row.iloc[0]["id"]) if not state_row.empty else None
                    )
                    st.rerun()
                else:
                    st.error(
                        "❌ Phone number not found. "
                        "Contact your supervisor to get registered."
                    )

        st.divider()
        st.caption("Having trouble logging in? Contact your supervisor.")

    return False, None


def agent_logout():
    """Clear agent session."""
    for key in ["agent_authenticated", "agent_id",
                "agent_name", "agent_state", "agent_state_id"]:
        st.session_state[key] = None
    st.session_state.agent_authenticated = False
