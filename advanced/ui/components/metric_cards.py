"""Reusable monitoring KPI presentation."""

from __future__ import annotations

from collections.abc import Sequence

import streamlit as st


Metric = tuple[str, str, str]


def render_metric_cards(metrics: Sequence[Metric]) -> None:
    """Show responsive Streamlit metric tiles.

    Each tuple is ``(label, value, tooltip)``. Values should be formatted
    by the caller, so the component stays independent of business logic.
    """
    if not metrics:
        return

    for chunk_start in range(0, len(metrics), 5):
        chunk = metrics[chunk_start:chunk_start + 5]
        columns = st.columns(len(chunk), gap="small")
        for column, (label, value, help_text) in zip(columns, chunk):
            with column:
                st.metric(label=label, value=value, help=help_text)
