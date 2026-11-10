"""Reusable Altair visualizations with a consistent monitoring palette."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


PERSON_COLOR = "#54c9b6"
PLATE_COLOR = "#7d9af9"
REVIEW_COLOR = "#f0b46b"
GRID_COLOR = "#28384e"
LABEL_COLOR = "#9fb2c9"

_SOURCE_SCALE = alt.Scale(
    domain=["Person", "Plate"],
    range=[PERSON_COLOR, PLATE_COLOR],
)


def _chart_base(chart: alt.Chart) -> alt.Chart:
    return chart.configure_view(strokeWidth=0).configure_axis(
        labelColor=LABEL_COLOR,
        titleColor=LABEL_COLOR,
        gridColor=GRID_COLOR,
        domainColor=GRID_COLOR,
        tickColor=GRID_COLOR,
        labelFontSize=11,
        titleFontSize=11,
    ).configure_legend(
        labelColor=LABEL_COLOR,
        titleColor=LABEL_COLOR,
        orient="top",
        direction="horizontal",
        symbolType="circle",
    )


def activity_chart(hourly: pd.DataFrame) -> None:
    """Render an hourly observation chart without inventing missing data."""
    if hourly.empty:
        st.info("No detection observations in the selected period.")
        return

    data = hourly.copy()
    data["hour"] = pd.to_datetime(data["hour"], errors="coerce")
    data = data.dropna(subset=["hour"])
    data["source"] = data["detection_source"].map(
        {"person": "Person", "plate": "Plate"}
    )
    data = data.dropna(subset=["source"])
    if data.empty:
        st.info("No chartable detection observations in this period.")
        return

    chart = (
        alt.Chart(data)
        .mark_line(point=alt.OverlayMarkDef(size=35), strokeWidth=2.7)
        .encode(
            x=alt.X("hour:T", title="Time (UTC)", axis=alt.Axis(format="%d %b %H:%M")),
            y=alt.Y("observation_count:Q", title="Observations", scale=alt.Scale(zero=True)),
            color=alt.Color("source:N", scale=_SOURCE_SCALE, title=None),
            tooltip=[
                alt.Tooltip("hour:T", title="Hour (UTC)", format="%Y-%m-%d %H:%M"),
                alt.Tooltip("source:N", title="Source"),
                alt.Tooltip("observation_count:Q", title="Observations", format=",d"),
            ],
        )
        .properties(height=305)
    )
    st.altair_chart(_chart_base(chart), use_container_width=True)


def zone_chart(zones: pd.DataFrame) -> None:
    """Render grouped counts by camera zone and observation source."""
    if zones.empty:
        st.info("No zone activity in the selected period.")
        return

    data = zones.copy()
    data["source"] = data["detection_source"].map(
        {"person": "Person", "plate": "Plate"}
    )
    data = data.dropna(subset=["source"])
    if data.empty:
        st.info("No chartable zone activity in this period.")
        return

    chart = (
        alt.Chart(data)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Zone:N", title="Zone", sort=["A", "B", "C", "D", "E"], axis=alt.Axis(labelAngle=0)),
            xOffset="source:N",
            y=alt.Y("observation_count:Q", title="Observations", scale=alt.Scale(zero=True)),
            color=alt.Color("source:N", scale=_SOURCE_SCALE, title=None),
            tooltip=["Zone:N", "source:N", alt.Tooltip("observation_count:Q", format=",d")],
        )
        .properties(height=305)
    )
    st.altair_chart(_chart_base(chart), use_container_width=True)


def single_zone_chart(zones: pd.DataFrame, color: str = REVIEW_COLOR) -> None:
    """Render a single-series zone count chart."""
    if zones.empty:
        st.info("No observations in the selected period.")
        return

    chart = (
        alt.Chart(zones)
        .mark_bar(color=color, cornerRadiusEnd=5)
        .encode(
            y=alt.Y("Zone:N", sort="ascending", title=None),
            x=alt.X("observation_count:Q", title="Observations"),
            tooltip=["Zone:N", alt.Tooltip("observation_count:Q", title="Observations", format=",d")],
        )
        .properties(height=245)
    )
    st.altair_chart(_chart_base(chart), use_container_width=True)
