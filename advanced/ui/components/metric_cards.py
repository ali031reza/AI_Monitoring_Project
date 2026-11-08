from collections.abc import Sequence

import streamlit as st


Metric = tuple[str, str, str]


def render_metric_cards(metrics: Sequence[Metric]) -> None:
    """Render a row of Streamlit metric cards.

    Each metric is (label, value, help_text).
    """
    columns = st.columns(len(metrics))

    for column, (label, value, help_text) in zip(columns, metrics):
        with column:
            st.metric(
                label=label,
                value=value,
                help=help_text,
            )
