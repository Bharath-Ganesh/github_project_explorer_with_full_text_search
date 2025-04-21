import os
import json
import tempfile
import pandas as pd
import streamlit as st
from pyvis.network import Network

from project_utils.github_utils import get_forks_from_semester_csv
from project_utils.search_utils import search_projects
from project_utils.graph_utils import generate_graph
from project_utils.starter_class import ProjectStarterClass

# === Initialize configuration and logger ===
CTX = ProjectStarterClass(name=__name__)

# === Load or trigger data fetch ===
if not os.path.exists(CTX.data_file):
    CTX.logger.warning("Project data not found. Prompting user to fetch projects.")
    st.warning("Project data not found. Click the button below to fetch and parse all projects.")

    if st.button("Fetch Projects Now"):
        projects = get_forks_from_semester_csv()
        os.makedirs(os.path.dirname(CTX.data_file), exist_ok=True)
        with open(CTX.data_file, "w", encoding="utf-8") as f:
            json.dump(projects, f, indent=2)
        CTX.logger.info(f"Saved {len(projects)} projects to {CTX.data_file}")
        st.success("Saved projects. Please reload the app.")
        st.stop()
    else:
        CTX.logger.info("User chose not to fetch new data.")
        st.stop()

# === Load project snapshot ===
with open(CTX.data_file, "r", encoding="utf-8") as f:
    all_projects = json.load(f)
CTX.logger.info(f"Loaded {len(all_projects)} projects from JSON.")

# === Title ===
st.title(CTX.app_ui.get("title", "Project Explorer"))

# === Input Filters ===
filter_inputs = {}
columns = st.columns(len(CTX.filters))
for idx, (key, meta) in enumerate(CTX.filters.items()):
    with columns[idx]:
        label = meta["label"]
        field = meta["field"]
        if meta.get("type") == "dropdown":
            value = st.selectbox(label, meta.get("options", []), key=field)
        else:
            value = st.text_input(label, key=field)
        filter_inputs[field] = value

# === Trigger Search ===
if st.button("Search"):
    CTX.logger.info(f"Search triggered with filters: {filter_inputs}")

    # Start with keyword filtering
    keyword = filter_inputs.get("keyword", "")
    filtered_projects = search_projects(all_projects, keyword)
    CTX.logger.info(f"Found {len(filtered_projects)} projects after keyword search.")

    # Apply remaining filters
    for field, value in filter_inputs.items():
        if field == "keyword" or not value:
            continue
        before = len(filtered_projects)
        filtered_projects = [p for p in filtered_projects if value.lower() in str(p.get(field, "")).lower()]
        CTX.logger.info(f"Filtered by '{field}': {before} → {len(filtered_projects)}")

    # === Display Matching Projects ===
    if filtered_projects:
        # Construct filter summary for logging/debug purposes
        filter_summary = " | ".join([f"{k}={v}" for k, v in filter_inputs.items() if v])
        CTX.logger.info(f"Displaying results with filters: {filter_summary}")

        # Display just the count in UI (cleaned up - no keyword shown)
        st.subheader(f"{len(filtered_projects)} Matching Projects")

        df = pd.DataFrame(filtered_projects)

        # Fallback renaming if repo_url key isn't present
        if "repo_url" not in df.columns and "html_url" in df.columns:
            df.rename(columns={"html_url": "repo_url"}, inplace=True)

        # === Extract display column metadata ===
        column_meta = [col for col in CTX.display_columns if isinstance(col, dict) and "field" in col]
        column_fields = [col["field"] for col in column_meta]
        column_labels = {col["field"]: col.get("label", col["field"]) for col in column_meta}
        link_columns = {col["field"] for col in column_meta if col.get("link")}
        max_widths = {col["field"]: col.get("max_width") for col in column_meta}

        # Reduce to only requested columns
        df = df[column_fields]


        # === Format cell contents ===
        def format_cell(value, field):
            if isinstance(value, list):
                return ", ".join(map(str, value))
            if field in link_columns and pd.notna(value):
                return f'<a href="{value}" target="_blank">{value}</a>'
            return str(value)


        for col in column_fields:
            df[col] = df[col].apply(lambda v: format_cell(v, col))

        # Apply pretty labels
        df.rename(columns=column_labels, inplace=True)


        # === Generate styled HTML Table ===
        def generate_styled_html(df, table_styles, display_columns):
            default_table_style = {"width": "95%", "border-collapse": "collapse", "margin-top": "20px",
                                   "table-layout": "fixed"}
            configured_table_style = table_styles.get('table', {})
            combined_table_style = {**default_table_style, **configured_table_style, "margin-left": "auto",
                                    "margin-right": "auto"}

            table_style_str = "; ".join([f"{key}: {value}" for key, value in combined_table_style.items()])

            cell_styles = table_styles.get('cell', {})
            cell_style_str = "; ".join([f"{key}: {value}" for key, value in cell_styles.items()])

            # Find the index of the 'repo_url' column
            repo_url_index = -1
            for idx, col_config in enumerate(display_columns):
                if isinstance(col_config, dict) and col_config.get('field') == 'repo_url':
                    repo_url_index = idx
                    break

            # Construct more specific CSS
            specific_styles = ""
            if repo_url_index != -1:
                specific_styles = f"""
                    table td:nth-child({repo_url_index + 1}) {{
                        word-wrap: break-word !important;
                        word-break: break-all !important;
                    }}
                """

            styles = f"""<style>
                table {{ {table_style_str} }}
                th, td {{ {cell_style_str} }}
                {specific_styles}
            </style>"""

            # Start building the HTML table
            html = styles + "<table><thead><tr>"
            # Add table headers with dynamic widths
            column_fields = df.columns
            column_config_map = {col['field']: col for col in display_columns if
                                 isinstance(col, dict) and 'field' in col}

            for col in column_fields:
                header_style = ''
                if col in column_config_map and column_config_map[col].get('max_width'):
                    max_width = column_config_map[col]['max_width']
                    header_style = f'style="max-width: {max_width}; width: {max_width};"'
                elif col in column_config_map and CTX.default_column_width:
                    header_style = f'style="width: {CTX.default_column_width};"'  # Apply default if no specific max_width

                html += f"<th {header_style}>{col}</th>"
            html += "</tr></thead><tbody>"

            # Add table data rows
            for index, row in df.iterrows():
                html += "<tr>"
                for col_name, item in row.items():
                    data_style = ''
                    if col_name == 'repo_url':
                        data_style = 'style="word-wrap: break-word !important; word-break: break-all !important;"'
                    elif col_name in column_config_map and column_config_map[col_name].get('max_width'):
                        max_width_data = column_config_map[col_name]['max_width']
                        data_style = f'style="max-width: {max_width_data};"'
                    html += f"<td {data_style}>{item}</td>"
                html += "</tr>"

            html += "</tbody></table>"
            return html


        # === Render styled table ===
        st.markdown(
            generate_styled_html(df, CTX.table_styles, CTX.display_columns),
            unsafe_allow_html=True
        )

        # === Visualize Network ===
        G = generate_graph(filtered_projects)
        net = Network(
            height=CTX.graph_options["height"],
            width=CTX.graph_options["width"],
            bgcolor=CTX.graph_options["bgcolor"],
            font_color=CTX.graph_options["font_color"]
        )
        net.from_nx(G)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
            net.save_graph(tmp_file.name)
            st.components.v1.html(tmp_file.read().decode(), height=650)

    else:
        st.warning("No matching projects found.")
        CTX.logger.warning("Search yielded 0 results.")
