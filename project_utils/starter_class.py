import yaml

from project_utils.logger_setup import setup_logger, get_logger


class ProjectStarterClass:
    def __init__(self, name, config_path="config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()

        # Set up logging
        setup_logger()
        self.logger = get_logger(name)

        # === Extract config variables with fallbacks ===

        # Data path to load/save projects
        self.data_file = self.config.get("output_json", "data/projects.json")

        # Display config section for columns (list of dicts)
        raw_display = self.config.get("display_columns", [])

        # Extract and separate table styles and column list
        self.table_styles = {
            "table": "width: 100%; table-layout: fixed;",
            "cell": "word-wrap: break-word; padding: 6px;"
        }
        self.default_column_width = self.config.get("default_column_width", "200px")

        if raw_display and isinstance(raw_display[0], dict) and "table_styles" in raw_display[0]:
            self.table_styles = raw_display[0]["table_styles"]
            self.display_columns = raw_display[1:]  # Remaining entries are actual columns
        else:
            self.display_columns = raw_display

        # Graph rendering options
        self.graph_options = self.config.get("graph_options", {
            "height": "600px",
            "width": "100%",
            "bgcolor": "#ffffff",
            "font_color": "black"
        })

        # UI metadata like title or default messages
        self.app_ui = self.config.get("app", {
            "title": "Final Projects Explorer"
        })

        # Filter definitions for Streamlit input controls
        self.filters = {
            k: v for k, v in self.config.get("filters", {}).items()
            if v.get("enabled", True)
        }

    def _load_config(self):
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)