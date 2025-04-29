# === project_utils/starter_class.py ===
import os
import yaml
import logging
from pathlib import Path
from typing import Any, Dict, Optional

# -----------------------------------------------------------------------------
# ENV LOADING
# -----------------------------------------------------------------------------
def load_dotenv_file(dotenv_path: Path) -> None:
    """
    Parse a .env file at `dotenv_path` and set KEY=VALUE pairs
    into os.environ, without overwriting anything already present.
    """
    if not dotenv_path.exists():
        logging.getLogger(__name__).warning(f".env file not found at {dotenv_path}")
        return

    for raw in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('\'"'))

# Immediately load your .env from the project root
_repo_root = Path(__file__).resolve().parent.parent
load_dotenv_file(_repo_root / ".env")

# Now that we’ve loaded the .env, pull out your secrets
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
LOGGER_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Build HEADERS for the GitHub API only once, now that GITHUB_TOKEN exists
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}" if GITHUB_TOKEN else "",
    "Accept":        "application/vnd.github.v3+json"
}

# -----------------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------------
LOG_LEVELS = {
    "DEBUG":    logging.DEBUG,
    "INFO":     logging.INFO,
    "WARNING":  logging.WARNING,
    "ERROR":    logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

def setup_logger(log_file: Path = Path("logs") / "project_parser.log") -> None:
    """
    Configure root logger once: file + console handlers,
    using the level from LOG_LEVEL.
    """
    level = LOG_LEVELS.get(LOGGER_LEVEL, logging.INFO)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Basic file logging
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        filename=str(log_file),
        filemode="a",
    )

    # Console logging
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Return a named logger (with the above handlers already attached).
    """
    return logging.getLogger(name)

# -----------------------------------------------------------------------------
# Application Context
# -----------------------------------------------------------------------------
class AppContext:
    """
    Holds the merged configuration from config.yaml, exposes
    helper getters for nested keys, filters, fields, UI, etc.
    """
    def __init__(self, config: Dict[str, Any]):
        self._config = config

    def get(self, key: str, default: Any = None) -> Any:
        """Fetch dotted-path key, or return default if missing."""
        return self._resolve_key_path(key, default)

    def get_required(self, key: str) -> Any:
        """Fetch dotted-path key, or raise KeyError if missing."""
        val = self._resolve_key_path(key, None)
        if val is None:
            raise KeyError(f"Missing required config key: '{key}'")
        return val

    def get_required_keys(self, keys: set) -> Dict[str, Any]:
        """Fetch multiple top-level keys, erroring on any missing."""
        out = {}
        for k in keys:
            out[k] = self.get_required(k)
        return out

    def get_section(self, section: str) -> Dict[str, Any]:
        """Return a top-level dict section or {}."""
        return self._config.get(section, {})

    def get_fields(self) -> Dict[str, Any]:
        """Return the full mapping of 'fields' from config."""
        return self.get_required("fields")

    def get_filters(self) -> Dict[str, Any]:
        """Return only filters with `enabled: true`."""
        all_filters = self.get_section("filters")
        return {k: v for k, v in all_filters.items() if v.get("enabled", False)}

    def get_display_columns(self) -> list[Dict[str, Any]]:
        """
        Build the list of display‐column configs:
        - honor optional 'table_styles'
        - include only enabled fields
        """
        out: list[Dict[str, Any]] = []
        styles = self._config.get("table_styles")
        if styles:
            out.append({"table_styles": styles})

        for alias, meta in self.get_fields().items():
            if not meta.get("enabled", True):
                continue
            out.append({
                "field":      alias,
                "label":      meta.get("label", alias),
                "max_width":  meta.get("max_width"),
                "format":     meta.get("format"),
                "link":       meta.get("link", False),
                "wrap":       meta.get("wrap", True),
                "max_chars":  meta.get("max_chars"),
                "max_lines":  meta.get("max_lines"),
            })
        return out

    def get_app_ui(self) -> Dict[str, Any]:
        """Return the 'app' section for UIConfig."""
        return self.get_section("app")

    def _resolve_key_path(self, dotted: str, default: Any) -> Any:
        parts = dotted.split(".")
        cur = self._config
        for p in parts:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

# -----------------------------------------------------------------------------
# Context builder
# -----------------------------------------------------------------------------
_config_cache: Optional[AppContext] = None


def build_context(caller_name: str, config_path: Optional[Path] = None) -> AppContext:
    """
    1) Loads .env (once) from repo root
    2) Finds+parses config.yaml at the repo root (or given path)
    3) Caches+returns an AppContext
    """
    global _config_cache
    if _config_cache is None:
        # 1) Logging
        setup_logger()
        logger = get_logger(__name__)
        # 2) Repo root = two levels up from this file
        repo_root = Path(__file__).resolve().parent.parent

        # 3) Load .env
        load_dotenv_file(repo_root / ".env")

        # 4) Locate config.yaml
        cfg_file = config_path or (repo_root / "config.yaml")
        if not cfg_file.is_file():
            raise FileNotFoundError(f"Cannot find config.yaml at {cfg_file}")
        logger.info(f"Loading configuration from {cfg_file}")

        # 5) Parse YAML
        cfg_dict = yaml.safe_load(cfg_file.read_text(encoding="utf-8"))
        _config_cache = AppContext(cfg_dict)

    return _config_cache
