# === src/renderer.py ===
import streamlit as st
import pandas as pd
import tempfile
from project_utils.starter_class import build_context
from project_utils.graph_utils import generate_graph

class UIConfig:
    """
    Handles set_page_config() and page header.
    Must be invoked before any other st.* call.
    """
    def __init__(self, ctx=None):
        self.ctx    = ctx or build_context(self.__class__.__name__)
        self.app_ui = self.ctx.get_app_ui()

    def apply(self):
        st.set_page_config(
            page_title = self.app_ui.get("title", ""),
            layout     = self.app_ui.get("layout", "wide")
        )
        st.title(self.app_ui.get("title", ""))
        if self.app_ui.get("description"):
            st.write(self.app_ui["description"])


class Renderer:
    """
    Combines page setup (via UIConfig) with table/graph rendering.
    """
    def __init__(self, fields_cfg: list[dict], graph_options: dict, default_column_width: str):


        # 1) Pull missing‐column message from config
        app_ui = build_context(__name__).get_app_ui()
        self.missing_msg = app_ui.get("missing_column_msg", "Some columns are missing: {cols}")

        # 2) Unpack optional table_styles
        first = fields_cfg[0] if fields_cfg else {}
        if isinstance(first, dict) and "table_styles" in first:
            self.table_styles = first["table_styles"]
            self.fields = fields_cfg[1:]
        else:
            self.table_styles = {"table": {}, "cell": {}}
            self.fields        = fields_cfg

        self.graph_options        = graph_options
        self.default_column_width = default_column_width

    def render_table(self, rows: list[dict]):
        df = pd.DataFrame(rows)
        missing = [f["field"] for f in self.fields if f["field"] not in df.columns]
        if missing:
            st.warning(self.missing_msg.format(cols=", ".join(missing)))

        # Build CSS
        default_table = {
            "width": "auto",
            "border-collapse": "collapse",
            "margin-top": "20px",
            "table-layout": "fixed"
        }
        default_cell = {
            "overflow": "hidden",
            "text-overflow": "ellipsis",
            "white-space": "nowrap",
            "padding": "6px"
        }

        tbl_cfg  = {**default_table, **self.table_styles.get("table", {})}
        cell_cfg = {**default_cell,  **self.table_styles.get("cell",  {})}

        table_css = "; ".join(f"{k}: {v}" for k, v in tbl_cfg.items())
        cell_css  = "; ".join(f"{k}: {v}" for k, v in cell_cfg.items())

        # Header
        html = f'<table style="{table_css}"><thead><tr>'
        for f in self.fields:
            label = f.get("label", f["field"])
            max_w  = f.get("max_width", self.default_column_width)
            html  += f'<th style="max-width:{max_w};">{label}</th>'
        html += '</tr></thead><tbody>'

        # Rows
        for _, row in df.iterrows():
            html += '<tr>'
            for f in self.fields:
                fld = f["field"]
                val = row.get(fld, "")
                cell = "<br>".join(map(str, val)) if isinstance(val, list) else str(val)
                if f.get("link") and pd.notna(val):
                    cell = f'<a href="{val}" target="_blank">{val}</a>'
                max_w = f.get("max_width", self.default_column_width)
                html += f'<td style="max-width:{max_w}; {cell_css}">{cell}</td>'
            html += '</tr>'

        html += '</tbody></table>'
        st.markdown(html, unsafe_allow_html=True)

    def render_graph(self, rows: list[dict]):
        G = generate_graph(rows, **self.graph_options)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
            G.save_graph(tmp.name)
            st.components.v1.html(tmp.read().decode(), height=self.graph_options.get("height", 600))
