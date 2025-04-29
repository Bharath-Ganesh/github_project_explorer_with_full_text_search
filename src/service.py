# === src/service.py ===
from project_utils.postgres_uploader import PostgresUploader
from project_utils.starter_class import get_logger


class ProjectService:
    def __init__(self, dao, config):
        self.dao    = dao
        self.logger = get_logger(__name__)
        # assume config is the raw dict loaded by build_context(...)
        self.ui_page_limit  = config.get("pagination", {}).get("page_size", 30)
        # assume config is the raw dict loaded by build_context(...)
        self.db_limit  = config.get("pagination", {}).get("max_db_rows", 10000)

    def fetch_projects(self,
                       filters: dict,
                       display_columns: list[dict]) -> list[dict]:
        db_limit = self.db_limit
        rows = self.dao.search(filters,
                               [col["field"] for col in display_columns],
                               db_limit)
        self.logger.info("ProjectsDAO.fetch_projects(): retrieved %d rows from database", len(rows))
        return rows

    def ingest_projects(self, project_dicts: list[dict]):
        """
        Rebuild the `projects` table (DROP + CREATE + INDEXES)
        then bulk‐upsert via the DAO.ingest() you already have.
        """
        # 1) Recreate the schema
        uploader = PostgresUploader()
        uploader.apply_schema()

        # 2) Bulk‐upsert into projects table
        self.dao.ingest(project_dicts)
