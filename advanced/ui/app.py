"""Entry point for the AI Monitoring Streamlit console.

Run from the repository root:
    python -m streamlit run advanced/ui/app.py
"""

from __future__ import annotations

import streamlit as st

from advanced.ui.theme import apply_theme


# ============================================================
# 1. APPLICATION CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="AI Monitoring | Operations Console",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "AI Monitoring · Read-only operational analytics over persisted Gold tables.",
    },
)

apply_theme()


# ============================================================
# 2. PRIMARY NAVIGATION
# ============================================================

pages = {
    "OPERATIONS": [
        st.Page("pages/dashboard.py", title="Overview", icon="📊", default=True),
        st.Page("pages/events_alerts.py", title="Observation Explorer", icon="📋"),
        st.Page("pages/camera_monitoring.py", title="Camera Inventory", icon="📹"),
    ],
    "INVESTIGATIONS": [
        st.Page("pages/person_monitoring.py", title="Person Monitoring", icon="👤"),
        st.Page("pages/security_access.py", title="Access Review", icon="🛡️"),
        st.Page("pages/employee_connections.py", title="Co-observation", icon="🔗"),
    ],
}

with st.sidebar:
    st.markdown(
        '<div class="app-brand">◈ &nbsp; AI Monitoring</div>'
        '<div class="app-brand-detail">Security data operations console</div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="app-mini-label">WORKSPACE</div>', unsafe_allow_html=True)

selected_page = st.navigation(pages, position="sidebar")

with st.sidebar:
    st.markdown(
        '<div class="app-sidebar-line">'
        '<div class="app-mini-label">DATA CONNECTION</div>'
        '<div style="font-size:.84rem;color:#d4e2ef;margin-top:.4rem">'
        'Gold · Spark / Hive</div>'
        '<div style="font-size:.76rem;color:#8098b2;margin-top:.35rem">'
        'Persisted observations · Read only</div></div>',
        unsafe_allow_html=True,
    )
    st.caption("Data refresh is not a camera heartbeat or ETL job trigger.")

selected_page.run()
