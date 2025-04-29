# src/renderer.py

import streamlit as st
import pandas as pd
import tempfile
from project_utils.starter_class import setup_logger, get_logger, build_context


class UIConfig:
    """
    Handles set_page_config() and page header.
    Must be invoked before any other st.* call.
    """
    def __init__(self, ctx=None):
        setup_logger()
        self.logger = get_logger(self.__class__.__name__)

        self.ctx    = ctx or build_context(self.__class__.__name__)
        self.app_ui = self.ctx.get_app_ui()

        self.logger.info(
            "UIConfig:init title=%r layout=%r description=%r",
            self.app_ui.get("title"),
            self.app_ui.get("layout"),
            self.app_ui.get("description"),
        )

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
    Renders a table with:
     - per-column max_width
     - wrap/no-wrap
     - max_chars truncation
     - max_lines clamping
     - horizontal scrolling wrapper
    """
    def __init__(self, fields_cfg: list[dict], graph_options: dict, default_column_width: str):
        setup_logger()
        self.logger = get_logger(self.__class__.__name__)

        app_ui = build_context(self.__class__.__name__).get_app_ui()
        self.missing_msg = app_ui.get("missing_column_msg", "Some columns are missing: {cols}")

        # strip out optional table_styles
        first = fields_cfg[0] if fields_cfg else {}
        if isinstance(first, dict) and "table_styles" in first:
            self.table_styles = first["table_styles"]
            self.fields       = fields_cfg[1:]
        else:
            self.table_styles = {"table": {}, "cell": {}}
            self.fields       = fields_cfg

        self.default_w     = default_column_width
        self.graph_options = graph_options

        self.logger.info(
            "Renderer:init fields=%s default_w=%r",
            [f["field"] for f in self.fields],
            self.default_w
        )

    def render_table(self, rows: list[dict]):
        df = pd.DataFrame(rows)
        missing = [f["field"] for f in self.fields if f["field"] not in df.columns]
        if missing:
            st.warning(self.missing_msg.format(cols=", ".join(missing)))

        # outer table CSS
        tbl_cfg = {
            "width": "100%",
            "border-collapse": "collapse",
            "table-layout": "fixed",
            **self.table_styles.get("table", {})
        }
        tbl_css = "; ".join(f"{k}: {v}" for k, v in tbl_cfg.items())

        html = f'<div style="overflow-x:auto;"><table style="{tbl_css}"><thead><tr>'
        for f in self.fields:
            label = f.get("label", f["field"])
            max_w  = f.get("max_width", self.default_w)
            html  += f'<th style="max-width:{max_w};">{label}</th>'
        html += "</tr></thead><tbody>"

        for _, row in df.iterrows():
            html += "<tr>"
            for f in self.fields:
                alias = f["field"]
                raw   = row.get(alias, "")

                # flatten & truncate by max_chars
                if isinstance(raw, list):
                    parts = []
                    for itm in raw:
                        s = str(itm)
                        mc = f.get("max_chars")
                        if mc and len(s) > mc:
                            s = s[:mc].rstrip() + "…"
                        parts.append(s)
                    text = "<br>".join(parts)
                else:
                    s = str(raw)
                    mc = f.get("max_chars")
                    if mc and len(s) > mc:
                        s = s[:mc].rstrip() + "…"
                    text = s

                # turn into link
                if f.get("link") and isinstance(raw, str) and raw:
                    text = f'<a href="{raw}" target="_blank">{text}</a>'

                # build cell CSS
                cell_styles = {
                    **self.table_styles.get("cell", {}),
                    "padding": "6px",
                    "max-width": f.get("max_width", self.default_w),
                }

                max_lines = f.get("max_lines")
                if max_lines:
                    cell_styles.update({
                        "display": "-webkit-box",
                        "-webkit-box-orient": "vertical",
                        "-webkit-line-clamp": str(max_lines),
                        "overflow": "hidden",
                    })
                else:
                    if f.get("wrap", True):
                        cell_styles.update({
                            "white-space": "normal",
                            "word-wrap": "break-word",
                        })
                    else:
                        cell_styles.update({
                            "white-space": "nowrap",
                            "overflow": "hidden",
                            "text-overflow": "ellipsis",
                        })

                cell_css = "; ".join(f"{k}: {v}" for k, v in cell_styles.items())
                html += f'<td style="{cell_css}">{text}</td>'

            html += "</tr>"

        html += "</tbody></table></div>"
        st.markdown(html, unsafe_allow_html=True)

