"""
TradeFlow NG — Authentication Module
Admin:  username + password (st.secrets)
Agent:  agent_id (e.g. TFN-KW-001) + password
"""

import os
import streamlit as st


def _get_admin_credentials():
    valid_user = valid_pass = None
    try:
        if hasattr(st, 'secrets') and 'auth' in st.secrets:
            valid_user = st.secrets['auth'].get('admin_username')
            valid_pass = st.secrets['auth'].get('admin_password')
    except Exception:
        pass
    if not valid_user:
        valid_user = os.environ.get('ADMIN_USERNAME', 'admin')
    if not valid_pass:
        valid_pass = os.environ.get('ADMIN_PASSWORD', 'tradeflow2026')
    return valid_user, valid_pass


def _query(sql, params=()):
    """Always routes through db_adapter so all translations apply."""
    try:
        sys_path = os.path.join(os.path.dirname(__file__))
        import sys; sys.path.insert(0, sys_path)
        from db_adapter import query as adapter_query
        return adapter_query(sql, params)
    except Exception as e:
        st.error(f"Database error in auth: {e}")
        import pandas as pd
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════
# ADMIN LOGIN
# ═══════════════════════════════════════════════════════════

def require_admin_login():
    """Returns True if admin authenticated, False if showing form."""
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False
    if "admin_user" not in st.session_state:
        st.session_state["admin_user"] = None

    if st.session_state["admin_authenticated"] is True:
        with st.sidebar:
            st.divider()
            st.caption(f"Logged in as **{st.session_state.get('admin_user','Admin')}**")
            if st.button("🚪 Logout", key="admin_logout"):
                st.session_state["admin_authenticated"] = False
                st.session_state["admin_user"] = None
                st.rerun()
        return True

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        logo_path = os.path.join(
            os.path.dirname(__file__), '..', 'dashboard', 'assets', 'logo-full.png')
        if os.path.exists(logo_path):
            st.image(logo_path, width=200)
        else:
            st.markdown("## 🌾 TradeFlow NG")
        st.markdown("### Admin Login")
        st.caption("Internal Control Dashboard")
        st.divider()

        username = st.text_input("Username", placeholder="admin",
                                  key="auth_username_input")
        password = st.text_input("Password", type="password",
                                  placeholder="Enter your password",
                                  key="auth_password_input")

        if st.button("Login →", type="primary",
                     use_container_width=True, key="auth_login_button"):
            valid_user, valid_pass = _get_admin_credentials()
            if username.strip() == valid_user.strip() and \
               password.strip() == valid_pass.strip():
                st.session_state["admin_authenticated"] = True
                st.session_state["admin_user"] = username.strip()
                st.rerun()
            else:
                st.error("❌ Incorrect username or password.")
        st.divider()
        st.caption("Contact your system administrator if you need access.")
    return False


# ═══════════════════════════════════════════════════════════
# AGENT LOGIN  —  Agent ID + Password
# ═══════════════════════════════════════════════════════════

def require_agent_login():
    """
    Agent logs in with Agent ID (e.g. TFN-KW-001) + password.
    Phone number is stored in DB but NOT used for login.
    Returns (True, agent_dict) or (False, None).
    """
    defaults = {
        "agent_authenticated": False,
        "agent_id":            None,   # DB row id (integer)
        "agent_name":          None,
        "agent_state":         None,
        "agent_state_id":      None,
        "agent_agent_id":      None,   # Text ID e.g. TFN-KW-001
        "agent_market":        None,
        "agent_market_id":     None,
        "agent_phone":         None,
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

    if st.session_state["agent_authenticated"] is True:
        return True, {
            "id":         st.session_state["agent_id"],
            "name":       st.session_state["agent_name"],
            "state":      st.session_state["agent_state"],
            "state_id":   st.session_state["agent_state_id"],
            "agent_id":   st.session_state["agent_agent_id"],
            "market":     st.session_state["agent_market"],
            "market_id":  st.session_state["agent_market_id"],
            "phone":      st.session_state["agent_phone"],
        }

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        logo_path = os.path.join(
            os.path.dirname(__file__), '..', 'dashboard', 'assets', 'logo-full.png')
        if os.path.exists(logo_path):
            st.image(logo_path, width=200)
        else:
            st.markdown("## 🌾 TradeFlow NG")
        st.markdown("### Agent Login")
        st.caption("Enter your Agent ID and password to continue.")
        st.divider()

        agent_id_input = st.text_input(
            "Agent ID",
            placeholder="e.g. TFN-KW-001",
            key="agent_id_input",
            help="Your unique Agent ID. Contact your supervisor if you don't know it.",
        )
        password_input = st.text_input(
            "Password", type="password",
            placeholder="Your password",
            key="agent_pass_input",
        )

        if st.button("Login →", type="primary",
                     use_container_width=True, key="agent_login_button"):
            aid = agent_id_input.strip().upper()
            pwd = password_input.strip()

            if not aid or not pwd:
                st.warning("Please enter both your Agent ID and password.")
            else:
                agent = _query(
                    """SELECT a.id, a.full_name, a.agent_id, a.phone,
                              a.state_id, a.market_id,
                              s.name AS state_name,
                              m.name AS market_name
                       FROM   agents a
                       LEFT JOIN states  s ON a.state_id  = s.id
                       LEFT JOIN markets m ON a.market_id = m.id
                       WHERE  a.agent_id  = ?
                         AND  a.password  = ?
                         AND  a.is_active = 1""",
                    (aid, pwd),
                )

                if not agent.empty:
                    row = agent.iloc[0]
                    st.session_state["agent_authenticated"] = True
                    st.session_state["agent_id"]        = int(row["id"])
                    st.session_state["agent_name"]      = str(row["full_name"])
                    st.session_state["agent_state"]     = str(row.get("state_name", "—"))
                    st.session_state["agent_state_id"]  = (
                        int(row["state_id"]) if row.get("state_id") else None
                    )
                    st.session_state["agent_agent_id"]  = str(row["agent_id"])
                    st.session_state["agent_market"]    = str(row.get("market_name", "—"))
                    st.session_state["agent_market_id"] = (
                        int(row["market_id"]) if row.get("market_id") else None
                    )
                    st.session_state["agent_phone"]     = str(row.get("phone", ""))
                    st.rerun()
                else:
                    st.error(
                        "❌ Incorrect Agent ID or password. "
                        "Contact your supervisor if you need help."
                    )
        st.divider()
        st.caption("Having trouble? Contact your TradeFlow NG supervisor.")
    return False, None


# ═══════════════════════════════════════════════════════════
# LOGOUT
# ═══════════════════════════════════════════════════════════

def agent_logout():
    for key in [
        "agent_authenticated", "agent_id", "agent_name",
        "agent_state", "agent_state_id", "agent_agent_id",
        "agent_market", "agent_market_id", "agent_phone",
    ]:
        st.session_state[key] = None
    st.session_state["agent_authenticated"] = False
