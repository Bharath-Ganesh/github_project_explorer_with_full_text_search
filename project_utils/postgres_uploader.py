#!/usr/bin/env python
"""
streaming_postgres_uploader.py — Apply schema and then stream & batch‐insert
enriched + extracted project metadata into Postgres.

Flow:
 1. Read your “project_metadata.json” (config key = metadata).
 2. Apply your full DDL (DROP + CREATE + indexes).
 3. Stream the JSON one record at a time, re‐extract README/imports,
    then batch‐insert into Postgres, logging any record‐level or batch‐level errors.
"""
import ijson
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from project_utils.starter_class import setup_logger, get_logger, build_context
from project_utils.readme_parser import RepoMetadataExtractor


class PostgresUploader:
    """
    Handles the “Load” phase of the ETL:
      • Applies the Postgres schema (projects table + indexes).
      • Streams through project_data.json of metadata from Github.
      • Re‐parses any missing README/import data from disk(Readme parsing, Library Extraction).
      • Inserts data in batches via executemany, with detailed logging.
    """
    def __init__(self):
        """
        1) Initialize logging.
        2) Load DB & file configuration from config.yaml.
        3) Create SQLAlchemy engine.
        4) Initialize a RepoMetadataExtractor for README + import parsing.
        5) Prepare the INSERT statement template.
        """
        # ─── 1) Logging ───────────────────────────────────────────
        self.logger = get_logger(__name__)
        # Log exactly which file is running this init
        self.logger.info("Initializing PostgresUploader (file: %s)", Path(__file__).resolve())

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
           repository_url, libraries, created_at, last_updated_at, search_vector)
        VALUES
          (
            :owner, :repo, :title, :year, :semester,
            :team_members, :repository_url, :libraries, :created_at, :last_updated_at,
            to_tsvector(
              'english',
              coalesce(:title,'') || ' ' || coalesce(:readme_text,'')
            )
          );
        """)

    def apply_schema(self):
        """
        Apply the full DDL to rebuild the `projects` table:
          - Drops any existing `projects` table.
          - Creates the table with all columns and indexes.
        """
        ddl_sql = self.ddl_path.read_text(encoding="utf-8")
        with self.engine.begin() as conn:
            conn.execute(text(ddl_sql))
        self.logger.info("Applied schema from %s", self.ddl_path)

    def _insert_batch(self, conn, batch_params, batch_num: int):
        """
        Insert one batch of project‐param dicts into Postgres.

        Args:
            conn:     Active SQLAlchemy connection.
            batch_params: List[dict] of parameter dicts matching `self.insert_sql`.
            batch_num:     Integer batch index (for logging).
        """
        try:
            conn.execute(self.insert_sql, batch_params)
            self.logger.info("Inserted batch %d (%d projects)", batch_num, len(batch_params))
        except SQLAlchemyError as e:
            self.logger.error(
                "Batch %d insertion failed (%d projects): %s",
                batch_num, len(batch_params), e,
                exc_info=True
            )
            # swallow and continue

    def stream_and_insert(self, batch_size: int = 100):
        """
        Stream the enriched JSON file, re‐extract README/imports if needed,
        accumulate into batches, and insert each batch.

        Args:
            batch_size: number of records per INSERT batch.
        """
        path           = Path(self.enriched_json)
        batch          = []
        batch_num      = 0
        total_inserted = 0
        parse_errors   = 0

        with path.open("r", encoding="utf-8") as f, self.engine.begin() as conn:
            parser = ijson.items(f, "item")
            for raw in parser:
                owner = raw.get("owner") or "<unknown>"
                repo = raw.get("repo") or "<unknown>"

                # 1) Re‐run extraction to fill readme_text & libraries
                try:
                    full = self.extractor._process_repo(raw)
                except Exception as e:
                    parse_errors += 1
                    self.logger.error(
                        "Failed parsing repo %s/%s: %s",
                        owner, repo, e,
                        exc_info=True
                    )
                    continue

                # 2) Normalize semester & year
                sem = (full.get("semester") or "").strip().upper()
                try:
                    year_int = int(sem.split()[-1])
                except Exception:
                    year_int = None

                # 2) Ensure we have a README snippet
                raw_readme = full.get("readme_text", "")
                # if the online JSON had no snippet, load it from disk
                if not raw_readme and full.get("clone_path"):
                    readme_path = Path(full["clone_path"]) / "README.md"
                    if readme_path.exists():
                        try:
                            # pull out structured sections
                            _ = self.extractor.parse_readme_path(readme_path)
                            # capture first N lines of raw text
                            lines = readme_path.read_text(
                                encoding="utf-8", errors="ignore"
                            ).splitlines()
                            raw_readme = "\n".join(lines)
                        except Exception as e:
                            parse_errors += 1
                            self.logger.error(
                                "Failed loading README.md for %s/%s: %s",
                                owner, repo, e,
                                exc_info=True
                            )

                # 4) Extract title & team_members
                try:
                    sections   = (self.extractor.parse_readme_text(raw_readme)
                                  if raw_readme else {})
                    title_list = sections.get("title", [])
                    title      = " ".join(title_list).strip()[:100]
                    team       = [c.get("login") for c in full.get("contributors", []) if c.get("login")]
                    if not team:
                        team = sections.get("team_members", [])
                except Exception as e:
                    parse_errors += 1
                    self.logger.error(
                        "Failed extracting title/team for %s/%s: %s",
                        owner, repo, e,
                        exc_info=True
                    )
                    title = ""
                    team  = []


                # 5) Libraries
                libs = full.get("libraries", [])

                # 6) Build param dict for INSERT
                params = {
                    "owner":          owner,
                    "repo":           repo,
                    "title":          title,
                    "year":           year_int,
                    "semester":       sem,
                    "team_members":   team,
                    "repository_url": full.get("html_url"),
                    "libraries":      libs,
                    "created_at":     full.get("created_at"),
                    "last_updated_at":      full.get("pushed_at"),
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
        """
        Top‐level entrypoint:
          1) Apply schema (DROP + CREATE + indexes).
          2) Stream & batch‐insert all enriched records.
        """
        # A) Rebuild DB schema
        self.apply_schema()
        # B) Stream & batch-insert
        self.stream_and_insert(batch_size=100)
        self.logger.info("All done.")

if __name__ == "__main__":
    PostgresUploader().run()
