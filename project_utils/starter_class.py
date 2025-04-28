# === project_utils/starter_class.py ===
import os
import yaml
import logging
from pathlib import Path

from project_utils.env_utils import LOGGER_LEVEL

_config_cache = None

# Map of string levels to logging constants
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}


def setup_logger(log_file_path: str = "logs/project_parser.log"):
    """
    Configure global logging once at application startup.
    Logs to file and to console at the level defined by LOGGER_LEVEL.
    """
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    level_name = (LOGGER_LEVEL or "INFO").upper()
    level = LOG_LEVEL_MAP.get(level_name, logging.INFO)

    # BasicConfig for file logging
    logging.basicConfig(
        filename=log_file_path,
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=level
    )

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def get_logger(name: str = __name__, log_file: str = "project_processing.log") -> logging.Logger:
    """
    Return a module-level logger, adding a file handler if none exist.
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        level_name = (LOGGER_LEVEL or "INFO").upper()
        level = LOG_LEVEL_MAP.get(level_name, logging.INFO)
        logger.setLevel(level)

        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


class AppContext:
    """
    Central configuration context: provides access to app, fields, filters,
    UI, DB, DDL, etc., all loaded from config.yaml.
    """
    def __init__(self, config_dict: dict):
        self._config = config_dict

    def get(self, key: str, default=None):
        """Shallow get of top-level or dotted-path keys."""
        return self._resolve_key_path(key, default)

    def get_required(self, key: str):
        """
        Raise if missing; return value at key (dotted path).
        """
        val = self._resolve_key_path(key)
        if val is None:
            raise KeyError(f"Missing required config key: '{key}'")
        return val

    def get_required_keys(self, keys: set) -> dict:
        """
        Return a dict of required top-level sections keyed by the names in `keys`.
        Raises KeyError if any key is missing.
        """
        result = {}
        for k in keys:
            v = self._resolve_key_path(k)
            if v is None:
                raise KeyError(f"Missing required config section: '{k}'")
            result[k] = v
        return result

    def get_section(self, section: str) -> dict:
        """Return a top-level section dict (or empty dict)."""
        return self._config.get(section, {})

    def get_fields(self) -> dict:
        """Return full mapping from alias to field metadata."""
        return self.get_required('fields')

    def get_filters(self) -> dict:
        """Return only enabled filters by alias."""
        allf = self.get_section('filters')
        return {k: v for k, v in allf.items() if v.get('enabled', False)}

    def get_display_columns(self) -> list[dict]:
        """
        Build list of display-column dicts from fields metadata.
        Honors an optional 'table_styles' key in config.
        """
        cols = []
        styles = self._config.get('table_styles')
        if styles:
            cols.append({'table_styles': styles})
        for alias, meta in self.get_fields().items():
            if not meta.get('enabled', True):
                continue
            cfg = {
                'field': alias,
                'label': meta.get('label', alias),
                'max_width': meta.get('max_width'),
                'format': meta.get('format'),
                'link': meta.get('link', False)
            }
            cols.append(cfg)
        return cols

    def get_app_ui(self) -> dict:
        """UI metadata: title, description, messages, etc."""
        return self.get_section('app')

    def _resolve_key_path(self, dotted: str, default=None):
        parts = dotted.split('.')
        ref = self._config
        for p in parts:
            if isinstance(ref, dict) and p in ref:
                ref = ref[p]
            else:
                return default
        return ref


def build_context(caller_name: str, path: str = None) -> AppContext:
    """
    1) Load .env from repo root (if present).
    2) Locate config.yaml in the repo root.
    3) Cache and return an AppContext.
    """
    global _config_cache
    if _config_cache is None:
        # Project root = two levels up
        repo_root = Path(__file__).resolve().parent.parent

        # Manual .env loader
        dotenv_path = repo_root / ".env"
        if dotenv_path.exists():
            for line in dotenv_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                v = v.strip().strip('"\'')
                os.environ.setdefault(k.strip(), v)
        else:
            print(f"Warning: .env file not found at {dotenv_path}")

        # Locate config.yaml
        config_path = Path(path) if path else (repo_root / "config.yaml")
        if not config_path.is_file():
            raise FileNotFoundError(f"Cannot find config.yaml at {config_path}")

        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        _config_cache = AppContext(cfg)

    return _config_cache
