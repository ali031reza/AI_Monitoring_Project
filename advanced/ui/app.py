import streamlit as st


st.set_page_config(
    page_title="AI Monitoring",
    page_icon="📹",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = {
    "Monitoring": [
        st.Page(
            "pages/dashboard.py",
            title="Dashboard / Overview",
            icon="📊",
            default=True,
        ),
        st.Page(
            "pages/person_monitoring.py",
            title="Person Monitoring",
            icon="🧍",
        ),
        st.Page(
            "pages/security_access.py",
            title="Security & Access Control",
            icon="🔐",
        ),
        st.Page(
            "pages/camera_monitoring.py",
            title="Camera Monitoring",
            icon="📷",
        ),
        st.Page(
            "pages/employee_connections.py",
            title="Employee Connections",
            icon="🔗",
        ),
        st.Page(
            "pages/events_alerts.py",
            title="Events & Alerts",
            icon="🚨",
        ),
    ],
}

with st.sidebar:
    st.title("AI Monitoring")
    st.caption("Camera Monitoring Platform")
    st.divider()
    st.caption("Environment: Development")
    st.caption("Data source: Gold layer")
    st.caption("Integration status: In progress")

selected_page = st.navigation(pages, position="sidebar")
selected_page.run()
