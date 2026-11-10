"""Shared-frame co-observation explorer with explicit limitations."""

from __future__ import annotations

import altair as alt
import streamlit as st

from advanced.ui.components.metric_cards import render_metric_cards
from advanced.ui.data_access import TIME_RANGES, load_connection_data
from advanced.ui.theme import (
    empty_state, note, page_header, section_heading,
    show_data_error, source_caption,
)


def render_connections() -> None:
    page_header(
        "Investigations / Co-observation",
        "Employee co-observation",
        "Find pairs of employee IDs appearing in the same person-camera event. "
        "This describes shared detection frames—not communication, identity relationships, or direct interactions.",
    )
    period = st.selectbox("Time window", TIME_RANGES, key="connections_window")
    try:
        data = load_connection_data(period, limit=100)
    except Exception as exc:
        show_data_error(exc, "Co-observation")
        return

    source_caption(data["loaded_at"], period)
    pairs = data["pairs"]
    people = (
        set(pairs["employee_a"].tolist()) | set(pairs["employee_b"].tolist())
        if not pairs.empty else set()
    )
    render_metric_cards([
        (
            "Employee pairs shown", f"{len(pairs):,}", "Up to 100 most frequent co-observed pairs in the selected period."),
        (
            "Employees in shown pairs", f"{len(people):,}", "Unique employee IDs within the displayed pairs, not across all Gold records."),
        (
            "Shared frames in shown pairs", f"{int(pairs['shared_frames'].sum()) if not pairs.empty else 0:,}",
            "Sum of pair-frame appearances; one camera frame may contribute to multiple pairs."),
    ])

    st.write("")
    section_heading("Shared-frame pairs", "Ranked by co-observed frame count")
    if pairs.empty:
        empty_state("No co-observed pairs", "No frames contain two or more distinct detected employee IDs in this period.")
    else:
        table = pairs.rename(columns={
            "employee_a": "Employee A",
            "employee_b": "Employee B",
            "shared_frames": "Shared frames",
            "cameras": "Cameras",
            "last_observed_utc": "Last shared frame (UTC)",
        })
        st.dataframe(table, use_container_width=True, hide_index=True, height=390)

        st.write("")
        section_heading("Most frequently co-observed", "Top 12 displayed pairs")
        plot = table.head(12).copy()
        plot["Pair"] = plot["Employee A"] + "  /  " + plot["Employee B"]
        chart = (
            alt.Chart(plot)
            .mark_bar(color="#54c9b6", cornerRadiusEnd=5)
            .encode(
                x=alt.X("Shared frames:Q", title="Shared camera frames"),
                y=alt.Y("Pair:N", sort="-x", title=None),
                tooltip=["Pair:N", alt.Tooltip("Shared frames:Q", format=",d"), "Cameras:Q"],
            )
            .properties(height=max(240, 29 * len(plot)))
        )
        st.altair_chart(chart, use_container_width=True)

    note(
        "Pairs require two different person employee IDs in the same camera_id + event_id + timestamp + zone. "
        "Plate matches are excluded. Co-observation is not evidence of conversation, friendship, movement together, or any other relationship."
    )


render_connections()
