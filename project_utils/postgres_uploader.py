#!/usr/bin/env python
"""
postgres_uploader.py — Upload enriched + extracted metadata to Postgres

Flow:
 1. Read data/enriched_projects.json (contains clone_path & readme_text).
 2. For each record:
      • take the raw `readme_text` snippet
      • parse it via RepoMetadataExtractor.parse_readme_text
        to extract `title` & `team_members`
      • fallback to contributors’ logins if no team_members found
 3. Drop & recreate `projects` table via your SQL DDL.
 4. Insert rows (owner, repo, title, semester, team_members, repository_url, libraries, created_at)
    computing `search_vector` inline from title + readme_text.
"""
import json
from pathlib import Path

import psycopg2

from project_utils.logger_setup    import setup_logger, get_logger
from project_utils.readme_parser import RepoMetadataExtractor
from project_utils.starter_class   import build_context


class PostgresUploader:
    def __init__(self):
        setup_logger()
        self.logger = get_logger(__name__)

        # load DB credentials & enriched JSON path
        ctx = build_context(__name__)
        cfg = ctx.get_required_keys({"postgres", "enriched_json"})
        self.db_cfg   = cfg["postgres"]
        self.enriched = cfg["enriched_json"]

        # path to your table DDL
        self.ddl_path = Path(__file__).parent / "postgres_schema" / "projects_table.sql"

        # reuse one extractor instance
        self.extractor = RepoMetadataExtractor()

    def connect(self):
        return psycopg2.connect(
            dbname   = self.db_cfg["dbname"],
            user     = self.db_cfg["user"],
            password = self.db_cfg["password"],
            host     = self.db_cfg["host"],
            port     = self.db_cfg["port"],
        )

    def load_enriched(self):
        with open(self.enriched, "r", encoding="utf-8") as f:
            self.logger.info(f"Loaded enriched projects from {self.enriched}")
            return json.load(f)

    def drop_and_create(self, cur):
        self.logger.info("Dropping existing `projects` table, if any...")
        cur.execute("DROP TABLE IF EXISTS projects CASCADE;")

        self.logger.info(f"Creating `projects` table from {self.ddl_path}...")
        ddl = self.ddl_path.read_text(encoding="utf-8")
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt + ";")

    def run(self):
        enriched_list = self.load_enriched()

        conn = self.connect()
        cur  = conn.cursor()
        self.drop_and_create(cur)
        conn.commit()

        insert_sql = """
        INSERT INTO projects
          (owner, repo, title, semester, team_members, repository_url, libraries, created_at, search_vector)
        VALUES
          (
            %s,  -- owner
            %s,  -- repo
            %s,  -- title
            %s,  -- semester
            %s,  -- team_members (text[])
            %s,  -- repository_url
            %s,  -- libraries (text[])
            %s,  -- created_at
            to_tsvector(
              'english',
              coalesce(%s,'') || ' ' || coalesce(%s,'')
            )
          );
        """

        inserted = skipped = 0

        for proj in enriched_list:
            owner      = proj.get("owner")
            repo       = proj.get("repo")
            created_at = proj.get("created_at")
            libs       = proj.get("libraries", [])
            raw_readme = proj.get("readme_text", "")
            # new column name mapping
            repository_url = proj.get("repository_url", proj.get("html_url", ""))
            semester = proj.get("semester", "")

            if not raw_readme.strip():
                skipped += 1
                self.logger.warning(f"Skipping {owner!r}/{repo!r}: empty README snippet")
                continue

            sections = self.extractor.parse_readme_text(raw_readme)
            title_list = sections.get("title", [])
            title = " ".join(title_list).strip()[:100]

            readme_team = sections.get("team_members", [])
            team = readme_team if readme_team else [c.get("login") for c in proj.get("contributors", [])]

            params = [
                owner,
                repo,
                title,
                semester,
                team,
                repository_url,
                libs,
                created_at,
                title,        # for search_vector
                raw_readme,
            ]

            try:
                cur.execute(insert_sql, params)
                inserted += 1
                if inserted % 50 == 0:
                    conn.commit()
                    self.logger.info(f"Inserted {inserted} rows…")
            except Exception as e:
                self.logger.error(f"Failed to insert {owner!r}/{repo!r}: {e}")
                conn.rollback()

        conn.commit()
        self.logger.info(f"Done: {inserted} inserted, {skipped} skipped.")
        cur.close()
        conn.close()


if __name__ == "__main__":
    PostgresUploader().run()
