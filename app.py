#!/usr/bin/env python
# === src/app.py ===

# 0) Initialize logging at the very top
from project_utils.starter_class import setup_logger
setup_logger()

import streamlit as st
from project_utils.starter_class import build_context
from src.dao import ProjectsDAO
from src.service import ProjectService
from src.renderer import Renderer, UIConfig


def main():
    # 1) Load central config
    ctx = build_context(__name__)

    # 2) Apply global UI settings (must be first Streamlit call)
    UIConfig(ctx).apply()

    # 3) Build display_columns list from fields mapping
    fields_map      = ctx.get_fields()
    styles          = ctx.get("table_styles")
    display_columns = []
    if styles:
        display_columns.append({"table_styles": styles})
    for alias, meta in fields_map.items():
        if not meta.get("enabled", True):
            continue
        display_columns.append({"field": alias, **meta})

    # 4) Sidebar filters
    st.sidebar.header("Filters")
    filters_cfg   = ctx.get_filters()
    filter_inputs = {}
    for alias, cfg in filters_cfg.items():
        label = cfg.get("label", alias)
        if cfg.get("type") == "dropdown":
            filter_inputs[alias] = st.sidebar.selectbox(label, cfg.get("options", []))
        else:
            filter_inputs[alias] = st.sidebar.text_input(label)

    # 5) Business logic
    dao     = ProjectsDAO()
    service = ProjectService(dao, ctx._config)
    results = service.fetch_projects(filter_inputs, display_columns)

    # 6) Render results
    graph_opts    = ctx.get("graph_options", {})
    default_width = ctx.get("default_column_width", "250px")
    renderer      = Renderer(display_columns, graph_opts, default_width)
    renderer.render_table(results)
    # renderer.render_graph(results)  # toggle on if you want the graph


if __name__ == "__main__":
    main()
