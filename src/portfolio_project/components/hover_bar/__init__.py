from pathlib import Path
from typing import Sequence

import streamlit.components.v1 as components


_COMPONENT_PATH = Path(__file__).parent / "frontend"
_hover_bar = components.declare_component("hover_bar", path=str(_COMPONENT_PATH))


def hover_bar_chart(
    *,
    x: Sequence[float],
    y: Sequence[str],
    colors: Sequence[str],
    height: int,
    x_range: Sequence[float],
    key: str | None = None,
    open_in_new_tab: bool = False,
) -> str | None:
    return _hover_bar(
        x=list(x),
        y=list(y),
        colors=list(colors),
        height=int(height),
        x_range=list(x_range),
        chart_type="horizontal",
        open_in_new_tab=bool(open_in_new_tab),
        key=key,
    )


def hover_grouped_bar_chart(
    *,
    categories: Sequence[str],
    series: Sequence[dict],
    height: int,
    key: str | None = None,
    open_in_new_tab: bool = False,
    yaxis_title: str = "Sentiment / 5D Return (%)",
    yaxis2_title: str = "5D Return (%)",
    y_range: Sequence[float] | None = None,
    y2_range: Sequence[float] | None = None,
    bar_gap: float = 0.4,
    group_gap: float = 0.2,
) -> str | None:
    return _hover_bar(
        categories=list(categories),
        series=series,
        height=int(height),
        chart_type="grouped",
        yaxis_title=yaxis_title,
        yaxis2_title=yaxis2_title,
        y_range=list(y_range) if y_range is not None else None,
        y2_range=list(y2_range) if y2_range is not None else None,
        bar_gap=bar_gap,
        group_gap=group_gap,
        open_in_new_tab=bool(open_in_new_tab),
        key=key,
    )
