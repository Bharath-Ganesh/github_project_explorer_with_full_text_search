#!/usr/bin/env python
"""
streaming_postgres_uploader.py — Apply schema and then stream & batch-insert
enriched + extracted project metadata into Postgres.

Flow:
 1. Read your “final_projects.json” (config key = metadata).
 2. Apply your full DDL (DROP + CREATE + indexes).
 3. Stream the JSON one record at a time, re-extract README/imports,
    then batch-insert into Postgres.
"""
import ijson
from pathlib import Path

from sqlalchemy import create_engine, text

from project_utils.starter_class import setup_logger, get_logger, build_context
from project_utils.readme_parser import RepoMetadataExtractor


class PostgresUploader:
    def __init__(self):
        # ─── 1) Logging ───────────────────────────────────────────
        setup_logger()
        self.logger = get_logger(__name__)

        # ─── 2) Configuration ────────────────────────────────────
        ctx = build_context(__name__)
        pg = ctx.get_required("postgres")
        # JSON produced by RepoMetadataExtractor.run()
        self.enriched_json = ctx.get_required("metadata")
        # DDL file that drops/creates your `projects` table + indexes
        self.ddl_path = Path(__file__).parent / "postgres_schema" / "projects_table.sql"

        # ─── 3) SQLAlchemy engine ───────────────────────────────
        self.engine = create_engine(
            f"postgresql+psycopg2://{pg['user']}:{pg['password']}"
            f"@{pg['host']}:{pg['port']}/{pg['dbname']}"
        )

        # ─── 4) README + import extractor ────────────────────────
        self.extractor = RepoMetadataExtractor(metadata_json=self.enriched_json)

        # ─── 5) INSERT template ──────────────────────────────────
        self.insert_sql = text("""
        INSERT INTO projects
          (owner, repo, title, year, semester, team_members,
           repository_url, libraries, created_at, search_vector)
        VALUES
          (
            :owner, :repo, :title, :year, :semester,
            :team_members, :repository_url, :libraries, :created_at,
            to_tsvector(
              'english',
              coalesce(:title,'') || ' ' || coalesce(:readme_text,'')
            )
          );
        """)

    def apply_schema(self):
        """Run your full DDL (DROP + CREATE + indexes)."""
        ddl_sql = self.ddl_path.read_text(encoding="utf-8")
        with self.engine.begin() as conn:
            conn.execute(text(ddl_sql))
        self.logger.info("Applied schema from %s", self.ddl_path)

    def _insert_batch(self, conn, batch_params, batch_num: int):
        """Execute one batch of param-dicts via executemany."""
        conn.execute(self.insert_sql, batch_params)
        self.logger.info("Inserted batch %d (%d projects)", batch_num, len(batch_params))

    def stream_and_insert(self, batch_size: int = 100):
        """
        Stream the JSON file, re-extract any missing README/import data,
        accumulate into batches, and insert each batch.
        """
        path           = Path(self.enriched_json)
        batch          = []
        batch_num      = 0
        total_inserted = 0

        with path.open("r", encoding="utf-8") as f, self.engine.begin() as conn:
            parser = ijson.items(f, "item")
            for raw in parser:
                # 1) Re-run extraction on clone_path to fill readme_text & libraries
                full = self.extractor._process_repo(raw)

                # 2) Normalize semester/year
                sem = (full.get("semester") or "").strip().upper()
                try:
                    year_int = int(sem.split()[-1])
                except:
                    year_int = None

                # 2) Ensure we have a README snippet
                raw_readme = full.get("readme_text", "")
                # if the online JSON had no snippet, load it from disk
                if not raw_readme and full.get("clone_path"):
                    readme_path = Path(full["clone_path"]) / "README.md"
                    if readme_path.exists():
                        # use your parse_readme_path to pull out sections
                        sections = self.extractor.parse_readme_path(readme_path)
                        # but also take the first N raw lines for indexing
                        lines = readme_path.read_text(
                            encoding="utf-8", errors="ignore"
                        ).splitlines()
                        raw_readme = "\n".join(lines)

                # 4) Parse out title & team_members
                sections = self.extractor.parse_readme_text(raw_readme) if raw_readme else {}
                title_list = sections.get("title", [])
                title      = " ".join(title_list).strip()[:100]


                team = [c.get("login") for c in full.get("contributors", []) if c.get("login")]
                if not team:
                    team = sections.get("team_members", [])

                # 5) Libraries from extractor
                libs = full.get("libraries", [])

                # 6) Build param dict for INSERT
                params = {
                    "owner":          full.get("owner"),
                    "repo":           full.get("repo"),
                    "title":          title,
                    "year":           year_int,
                    "semester":       sem,
                    "team_members":   team,
                    "repository_url": full.get("repository_url") or full.get("html_url"),
                    "libraries":      libs,
                    "created_at":     full.get("created_at"),
                    "readme_text":    raw_readme,
                }

                batch.append(params)

                # 7) Flush batch when full
                if len(batch) >= batch_size:
                    self._insert_batch(conn, batch, batch_num)
                    total_inserted += len(batch)
                    batch_num      += 1
                    batch.clear()

            # 8) Final partial batch
            if batch:
                self._insert_batch(conn, batch, batch_num)
                total_inserted += len(batch)

        self.logger.info(
            "Streaming complete: %d batches, %d total projects inserted",
            batch_num + 1, total_inserted
        )

    def run(self):
        # A) Rebuild DB schema
        self.apply_schema()
        # B) Stream & batch-insert
        self.stream_and_insert(batch_size=100)
        self.logger.info("All done.")

if __name__ == "__main__":
    PostgresUploader().run()
