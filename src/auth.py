"""
TradeFlow NG — Authentication Module

Admin:  username + password (stored in st.secrets [auth])
Agents: phone number (looked up in agents table)

Usage:
    from auth import require_admin_login, require_agent_login, agent_logout
"""

import os
import streamlit as st


def _query(sql, params=()):
    """
    Auth-safe query — always routes through db_adapter
    so all SQLite→PostgreSQL translations apply automatically.
    """
    try:
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        from db_adapter import query as adapter_query
        return adapter_query(sql, params)
    except Exception as e:
        st.error(f"Database error in auth: {e}")
        import pandas as pd
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN LOGIN
# ══════════════════════════════════════════════════════════════════════════════

def require_admin_login():
    """
    Call at the top of app.py BEFORE any other content.
    Returns True if authenticated, False if showing the login form.
    """

    # ── Initialise session keys exactly once ──────────────────
    if "admin_authenticated" not in st.session_state:
        st.session_state["admin_authenticated"] = False
    if "admin_user" not in st.session_state:
        st.session_state["admin_user"] = None

    # ── Already authenticated — show logout and return ────────
    if st.session_state["admin_authenticated"] is True:
        with st.sidebar:
            st.divider()
            st.caption(
                f"Logged in as **{st.session_state.get('admin_user', 'Admin')}**"
            )
            if st.button("🚪 Logout", key="admin_logout"):
                st.session_state["admin_authenticated"] = False
                st.session_state["admin_user"] = None
                st.rerun()
        return True

    # ── Show login form ───────────────────────────────────────
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        logo_path = os.path.join(os.path.dirname(__file__), '..', 'dashboard', 'assets', 'TradeFlow profile.png')
        if os.path.exists(logo_path):
            st.image(logo_path, width=200)
        else:
            st.markdown("## 🌾 TradeFlow NG")
        st.markdown("### Admin Login")
        st.caption("Internal Control Dashboard")
        st.divider()

        # Use unique keys so Streamlit does not re-render on unrelated reruns
        username = st.text_input(
            "Username",
            placeholder="admin",
            key="auth_username_input"
        )
        password = st.text_input(
            "Password",
            type="password",
            placeholder="Enter your password",
            key="auth_password_input"
        )

        login_clicked = st.button(
            "Login →",
            type="primary",
            use_container_width=True,
            key="auth_login_button"
        )

        if login_clicked:
            # Read credentials from secrets (Streamlit Cloud) or env (local)
            try:
                valid_user = st.secrets["auth"]["admin_username"]
                valid_pass = st.secrets["auth"]["admin_password"]
            except Exception:
                valid_user = os.environ.get("ADMIN_USERNAME", "admin")
                valid_pass = os.environ.get("ADMIN_PASSWORD", "tradeflow2026")

            # Strip whitespace — prevents invisible character mismatch
            if username.strip() == valid_user.strip() and \
               password.strip() == valid_pass.strip():
                st.session_state["admin_authenticated"] = True
                st.session_state["admin_user"] = username.strip()
                st.rerun()
            else:
                st.error("❌ Incorrect username or password.")

        st.divider()
        st.caption("Contact your system administrator if you have forgotten your credentials.")

    return False


# ══════════════════════════════════════════════════════════════════════════════
# AGENT LOGIN
# ══════════════════════════════════════════════════════════════════════════════

def require_agent_login():
    """
    Call at the top of agent_app.py BEFORE any other content.
    Returns (True, agent_data_dict) or (False, None).
    """

    # ── Initialise session keys exactly once ──────────────────
    defaults = {
        "agent_authenticated": False,
        "agent_id":            None,
        "agent_name":          None,
        "agent_state":         None,
        "agent_state_id":      None,
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

    # ── Already authenticated ─────────────────────────────────
    if st.session_state["agent_authenticated"] is True:
        return True, {
            "agent_id":       st.session_state["agent_id"],
            "agent_name":     st.session_state["agent_name"],
            "agent_state":    st.session_state["agent_state"],
            "agent_state_id": st.session_state["agent_state_id"],
        }

    # ── Show login form ───────────────────────────────────────
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        logo_path = os.path.join(os.path.dirname(__file__), '..', 'dashboard', 'assets', 'TradeFlow dark.jpg')
        if os.path.exists(logo_path):
            st.image(logo_path, width=200)
        else:
            st.markdown("## 🌾 TradeFlow NG")
        st.markdown("### Agent Login")
        st.caption("Enter your registered phone number to continue.")
        st.divider()

        phone = st.text_input(
            "Phone number",
            placeholder="08012345678",
            key="agent_phone_input",
            help="The number you registered with your supervisor."
        )

        login_clicked = st.button(
            "Login →",
            type="primary",
            use_container_width=True,
            key="agent_login_button"
        )

        if login_clicked:
            phone_clean = phone.strip()
            if not phone_clean:
                st.warning("Please enter your phone number.")
            else:
                # Query uses db_adapter which applies is_active = TRUE translation
                agent = _query(
                    "SELECT * FROM agents WHERE phone = ? AND is_active = 1",
                    (phone_clean,)
                )

                if not agent.empty:
                    a = agent.iloc[0]

                    state_row = _query(
                        "SELECT id, name FROM states WHERE id = ?",
                        (int(a["state_id"]),)
                    )

                    st.session_state["agent_authenticated"] = True
                    st.session_state["agent_id"]            = int(a["id"])
                    st.session_state["agent_name"]          = str(a["full_name"])
                    st.session_state["agent_state"]         = (
                        str(state_row.iloc[0]["name"])
                        if not state_row.empty else "—"
                    )
                    st.session_state["agent_state_id"] = (
                        int(state_row.iloc[0]["id"])
                        if not state_row.empty else None
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


# ══════════════════════════════════════════════════════════════════════════════
# LOGOUT
# ══════════════════════════════════════════════════════════════════════════════

def agent_logout():
    """Clear agent session state completely."""
    for key in [
        "agent_authenticated", "agent_id",
        "agent_name", "agent_state", "agent_state_id"
    ]:
        st.session_state[key] = None
    st.session_state["agent_authenticated"] = False
