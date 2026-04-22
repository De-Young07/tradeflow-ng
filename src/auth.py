"""
TradeFlow NG — Authentication Module
Handles login for both admin and agent dashboards.

Admin:  username + password (from st.secrets)
Agents: phone number (must exist in agents table)

Usage:
    from auth import require_admin_login, require_agent_login
"""

import streamlit as st
import os

def _query(sql, params=()):
    """Query using the db_adapter — works on both SQLite and PostgreSQL."""
    try:
        # Load DATABASE_URL from Streamlit secrets if available
        try:
            os.environ["DATABASE_URL"] = st.secrets["database"]["DATABASE_URL"]
        except (KeyError, FileNotFoundError, AttributeError):
            pass

        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        from db_adapter import query as adapter_query
        return adapter_query(sql, params)
    except Exception as e:
        st.error(f"Database error in auth: {e}")
        import pandas as pd
        return pd.DataFrame()

# ══════════════════════════════════════════════════════════
# ADMIN LOGIN
# ══════════════════════════════════════════════════════════

def require_admin_login():
    # Must check BEFORE any other rendering
    if st.session_state.get("admin_authenticated") is True:
        with st.sidebar:
            st.divider()
            st.caption(f"Logged in as **{st.session_state.get('admin_user', 'Admin')}**")
            if st.button("🚪 Logout", key="admin_logout"):
                st.session_state["admin_authenticated"] = False
                st.session_state["admin_user"] = None
                st.rerun()
        return True

    # Show login form
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 🌾 TradeFlow NG")
        st.markdown("### Admin Login")
        st.divider()

        username = st.text_input("Username", key="login_user")
        password = st.text_input("Password", type="password", key="login_pass")

        if st.button("Login →", type="primary", use_container_width=True, key="login_btn"):
            try:
                valid_user = st.secrets["auth"]["admin_username"]
                valid_pass = st.secrets["auth"]["admin_password"]
            except Exception:
                valid_user = os.environ.get("ADMIN_USERNAME", "admin")
                valid_pass = os.environ.get("ADMIN_PASSWORD", "tradeflow2026")

            if username.strip() == valid_user and password.strip() == valid_pass:
                st.session_state["admin_authenticated"] = True
                st.session_state["admin_user"] = username.strip()
                st.rerun()
            else:
                st.error("❌ Incorrect username or password.")

    return False

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

        if st.button("Login →", type="primary", width='stretch'):
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

        if st.button("Login →", type="primary", width='stretch'):
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
