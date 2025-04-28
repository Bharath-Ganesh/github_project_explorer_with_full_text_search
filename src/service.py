# === src/service.py ===
from src.dao import ProjectsDAO


class ProjectService:
    def __init__(self, dao: ProjectsDAO, cfg: dict):
        self.dao   = dao
        self.limit = cfg.get("default_limit", 50)

    def fetch_projects(self, filter_inputs: dict, display_columns: list[dict]) -> list[dict]:
        select_fields = [c["field"] for c in display_columns if "field" in c]
        return self.dao.search(filter_inputs, select_fields, self.limit)

    def ingest_projects(self, project_dicts: list[dict]):
        self.dao.ingest(project_dicts)