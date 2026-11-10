"""Shared presentation primitives for the monitoring console.

This module contains only Streamlit UI concerns; it does not query data.
"""

from __future__ import annotations

from html import escape

import streamlit as st


# ============================================================
# 1. VISUAL FOUNDATION
# ============================================================

_STYLE = """
<style>
:root { color-scheme: dark; }
.stApp {
    background:
      radial-gradient(circle at 89% 4%, rgba(23, 93, 121, .14), transparent 32%),
      radial-gradient(circle at 7% 0%, rgba(57, 71, 133, .13), transparent 29%),
      #0b111d;
    color: #e7ecf5;
}
.block-container { max-width: 1580px; padding-top: 2rem; padding-bottom: 4rem; }
[data-testid="stSidebar"] {
    background: #0d1725;
    border-right: 1px solid #223147;
}
[data-testid="stSidebar"] .block-container { padding-top: 1.25rem; }
[data-testid="stSidebarNav"] a { border-radius: 10px; }
[data-testid="stSidebarNav"] a:hover { background: rgba(84, 181, 207, .1); }
h1, h2, h3 { letter-spacing: -.035em; color: #f4f7fb !important; }
h1 { font-size: 2.15rem !important; font-weight: 760 !important; }
h2 { font-size: 1.4rem !important; font-weight: 720 !important; }
h3 { font-size: 1.1rem !important; font-weight: 670 !important; }
p, label, [data-testid="stCaptionContainer"] { color: #b8c6da; }
hr { border-color: #223248 !important; }
[data-testid="stMetric"] {
    background: linear-gradient(145deg, rgba(23,37,56,.98), rgba(16,28,45,.98));
    border: 1px solid #253951;
    border-radius: 16px;
    padding: 20px 20px 17px;
    min-height: 138px;
    box-shadow: 0 10px 34px rgba(0,0,0,.09);
}
[data-testid="stMetricLabel"] { color: #a6b8d0; font-size: .88rem; }
[data-testid="stMetricValue"] { color: #f6f9fd; font-weight: 760; letter-spacing: -.04em; }
[data-testid="stMetricDelta"] { color: #74d7be; }
[data-testid="stVerticalBlockBorderWrapper"] > div {
    border-color: #273b53 !important;
    border-radius: 15px !important;
}
[data-testid="stDataFrame"], [data-testid="stTable"] {
    border: 1px solid #263a50;
    border-radius: 12px;
    overflow: hidden;
}
[data-testid="stAlert"] { border-radius: 12px; }
[data-testid="stTabs"] [role="tablist"] { gap: 1rem; }
[data-testid="stTabs"] [role="tab"] { font-weight: 630; }
[data-testid="stButton"] button { border-radius: 9px; font-weight: 650; }
[data-testid="stDownloadButton"] button { border-radius: 9px; }
.app-eyebrow {
    text-transform: uppercase;
    letter-spacing: .16em;
    color: #67c9dc;
    font-size: .72rem;
    font-weight: 760;
    margin-bottom: .52rem;
}
.app-hero {
    padding: 1.55rem 1.85rem;
    border: 1px solid #29405a;
    border-radius: 20px;
    background: linear-gradient(116deg, rgba(25, 50, 73, .96), rgba(16, 29, 48, .96) 54%, rgba(24, 52, 72, .87));
    box-shadow: 0 18px 52px rgba(0,0,0,.13);
    margin-bottom: 1.25rem;
}
.app-hero h1 { margin: 0 0 .42rem; font-size: 2.3rem !important; line-height: 1.15; }
.app-hero p { margin: 0; color: #bfcede; font-size: .96rem; max-width: 780px; line-height: 1.65; }
.app-hero .app-eyebrow { margin-bottom: .55rem; }
.app-brand {
    font-weight: 760; color: #f7fafc; font-size: 1.3rem; letter-spacing: -.04em;
    margin-bottom: .2rem;
}
.app-brand-detail { font-size: .81rem; color: #8fa6c1; margin-bottom: 1.25rem; }
.app-mini-label {
    color: #8ea5c0; text-transform: uppercase; letter-spacing: .12em;
    font-size: .67rem; font-weight: 740;
}
.app-note {
    color: #9aadc4; font-size: .82rem; line-height: 1.55;
    border-left: 2px solid #386b83;
    padding: .45rem .95rem;
    margin: .85rem 0 1.2rem;
}
.app-section-head { display:flex; justify-content: space-between; align-items: baseline; gap:1rem; }
.app-section-head strong { color: #f2f7ff; font-size: 1.02rem; }
.app-section-head span { color: #92a7be; font-size: .78rem; }
.app-pill {
    display: inline-block; border-radius: 999px;
    padding: .29rem .65rem; margin-right: .38rem; margin-bottom: .4rem;
    border: 1px solid #2b4961; background: #152b3d; color: #b7d7e7;
    font-size: .76rem; font-weight: 630;
}
.app-pill-muted { background: #1b2431; border-color: #344052; color: #b8c4d5; }
.app-empty {
    padding: 2.2rem 1.1rem;
    border: 1px dashed #334a62;
    border-radius: 15px;
    text-align: center;
    color: #aebfd2;
    background: rgba(24,37,53,.48);
}
.app-empty strong { color: #f0f5fc; display:block; margin-bottom:.3rem; }
.app-sidebar-line { border-top: 1px solid #273c51; padding-top: .85rem; margin-top:.7rem; }
</style>
"""


def apply_theme() -> None:
    """Install shared local styles on the current Streamlit page."""
    st.markdown(_STYLE, unsafe_allow_html=True)


def page_header(eyebrow: str, title: str, description: str) -> None:
    """Render a consistent descriptive page header."""
    st.markdown(
        '<div class="app-hero">'
        f'<div class="app-eyebrow">{escape(eyebrow)}</div>'
        f'<h1>{escape(title)}</h1>'
        f'<p>{escape(description)}</p>'
        '</div>',
        unsafe_allow_html=True,
    )


def section_heading(title: str, detail: str = "") -> None:
    st.markdown(
        '<div class="app-section-head">'
        f'<strong>{escape(title)}</strong><span>{escape(detail)}</span>'
        '</div>',
        unsafe_allow_html=True,
    )


def note(message: str) -> None:
    st.markdown(
        f'<div class="app-note">{escape(message)}</div>',
        unsafe_allow_html=True,
    )


def empty_state(title: str, description: str) -> None:
    st.markdown(
        '<div class="app-empty">'
        f'<strong>{escape(title)}</strong>{escape(description)}</div>',
        unsafe_allow_html=True,
    )


def source_caption(loaded_at, time_range: str) -> None:
    """Label refresh as persisted Gold-data refresh, not camera health."""
    stamp = loaded_at.strftime("%Y-%m-%d %H:%M:%S")
    st.caption(
        f"Source: persisted Gold layer  ·  Window: {time_range}  ·  "
        f"Data retrieved: {stamp} UTC"
    )


def show_data_error(exc: Exception, section: str) -> None:
    st.error(f"Unable to load {section.lower()} from the Gold layer.")
    st.info(
        "Check that the Bronze → Silver → Gold ETL has completed and that "
        "the local Spark/Hive metastore is available."
    )
    with st.expander("Technical details"):
        st.exception(exc)
