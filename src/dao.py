# src/dao.py
import psycopg2
from psycopg2.extras import RealDictCursor

from project_utils.starter_class import build_context, get_logger


class ProjectsDAO:
    """
    Data‐access object for the `projects` table.

    Responsibilities:
      - Ingest (upsert) enriched project metadata.
      - Search projects with optional full‐text, year, semester, author, and library filters.
      - Map application‐level field aliases to DB columns.
    """

    def __init__(self):
        # Configure logging
        self.logger = get_logger(__name__)

        # Load configuration
        ctx = build_context(__name__)
        pg = ctx.get_required('postgres')
        self.table = pg['table']
        self.fts_column = pg['fts_column']
        self.fields = ctx.get_section('fields')

        self.logger.info("ProjectsDAO initialized for table %r (fts_column=%r)",
                         self.table, self.fts_column)

    def _connect(self):
        """Open a new psycopg2 connection using configured credentials."""
        pg = build_context(__name__).get_required('postgres')
        return psycopg2.connect(
            dbname=pg['dbname'],
            user=pg['user'],
            password=pg['password'],
            host=pg['host'],
            port=pg['port'],
        )

    def search(self, filters: dict, select_aliases: list[str], limit: int):
        """
        Run a filtered search against `projects`.

        :param filters:    dict of user‐supplied filters (keyword, year, library, author, semester)
        :param select_aliases: list of field aliases to include in the SELECT
        :param limit:      maximum number of rows to return
        :returns:          list of dicts, one per matching project
        """
        self.logger.debug("Starting search with filters=%s, select=%s, limit=%d",
                          filters, select_aliases, limit)
        # 1) Build SELECT clause
        select_clause = ", ".join(
            f"{self.fields[a]['column']} AS {a}"
            for a in select_aliases if a in self.fields
        ) or "*"
        parts = [f"SELECT {select_clause} FROM {self.table}"]
        where, params = [], []

        # 2) Apply each filter helper
        self._apply_author_filter(filters, where, params)
        self._apply_keyword_filter(filters, where, params)
        self._apply_library_filter(filters, where, params)
        self._apply_year_filter(filters, where, params)
        self._apply_semester_filter(filters, where, params)

        if where:
            parts.append("WHERE " + " AND ".join(where))
            self.logger.debug("WHERE clauses: %s", where)

        # 3) ORDER BY
        if filters.get("_kw_clean"):
            parts.append(
                f"ORDER BY ts_rank({self.fts_column}, plainto_tsquery('english', %s)) DESC,"
                " created_at DESC"
            )
            params.append(filters["_kw_clean"])
        else:
            parts.append("ORDER BY created_at DESC")

        # 4) LIMIT
        parts.append("LIMIT %s")
        params.append(limit)

        query = " ".join(parts)
        self.logger.debug("Final query: %s; params=%s", query, params)

        # 5) Execute
        with self._connect() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            cur.close()

        self.logger.info("Search returned %d rows", len(rows))
        return rows

    def ingest(self, project_dicts: list[dict]):
        """
        Bulk upsert of projects via INSERT ... ON CONFLICT.
        """
        insert_sql = """
        INSERT INTO projects
          (owner, repo, title, semester, team_members, repository_url,
           libraries, created_at, last_updated_at, search_vector)
        VALUES
          (%(owner)s, %(repo)s, %(title)s, %(semester)s,
           %(team_members)s, %(repository_url)s,
           %(libraries)s, %(created_at)s,%(last_updated_at)s
           to_tsvector('english',
             coalesce(%(title)s,'') || ' ' || coalesce(%(readme_text)s,'')))
        ON CONFLICT (owner, repo) DO UPDATE
          SET title          = EXCLUDED.title,
              semester       = EXCLUDED.semester,
              team_members   = EXCLUDED.team_members,
              repository_url = EXCLUDED.repository_url,
              libraries      = EXCLUDED.libraries,
              created_at     = EXCLUDED.created_at,
              last_updated_at = EXCLUDED.last_updated_at,
              search_vector  = EXCLUDED.search_vector;
        """
        self.logger.info("Ingesting %d projects", len(project_dicts))
        conn = self._connect()
        cur = conn.cursor()
        for proj in project_dicts:
            try:
                cur.execute(insert_sql, proj)
            except Exception as e:
                self.logger.error("Failed to ingest %r/%r: %s",
                                  proj.get("owner"), proj.get("repo"), e,
                                  exc_info=True)
        conn.commit()
        cur.close()
        conn.close()
        self.logger.info("Ingest complete")

    # ─── Private filter builders ─────────────────────────────────────────────────

    def _apply_author_filter(self, filters, where, params):
        """
        Filter on team_members full‐text match of an author string.
        """
        val = filters.get("author", "")
        if not isinstance(val, str) or not val.strip():
            return
        clean = val.strip()
        col = self.fields.get('author', {}).get('column', 'team_members')
        where.append(
            f"to_tsvector('english', array_to_string({col}, ' ')) "
            "@@ plainto_tsquery('english', %s)"
        )
        params.append(clean)
        self.logger.debug("Applied author filter: %r", clean)

    def _apply_keyword_filter(self, filters, where, params):
        """
        Full‐text search on the search_vector column.
        Supports quoted phrases via phraseto_tsquery.
        """
        kw = filters.get("keyword", "")
        if not isinstance(kw, str) or not kw.strip():
            return
        raw = kw.strip()
        if raw.startswith('"') and raw.endswith('"'):
            op, clean = "phraseto_tsquery", raw.strip('"')
        else:
            op, clean = "plainto_tsquery", raw
        where.append(f"{self.fts_column} @@ {op}('english', %s)")
        params.append(clean)
        filters["_kw_clean"] = clean
        self.logger.debug("Applied keyword filter: op=%s, term=%r", op, clean)

    def _apply_library_filter(self, filters, where, params):
        """
        Filter on overlap between libraries array and provided list.
        """
        val = filters.get("library") or filters.get("libraries") or ""
        if not isinstance(val, (str, list)) or (isinstance(val, str) and not val.strip()):
            return
        libs = val if isinstance(val, list) else [
            v.strip() for v in val.split(",") if v.strip()
        ]
        if not libs:
            return
        col = self.fields['libraries']['column']
        where.append(f"{col} && %s::text[]")
        params.append(libs)
        self.logger.debug("Applied library filter: %s", libs)

    def _apply_year_filter(self, filters, where, params):
        """
        Exact‐match filter on the year column.
        """
        val = filters.get("year", "")
        if not isinstance(val, str) or not val.strip():
            return
        try:
            year = int(val.strip())
        except ValueError:
            return
        col = self.fields.get('year', {}).get('column', 'year')
        where.append(f"{col} = %s")
        params.append(year)
        self.logger.debug("Applied year filter: %d", year)

    def _apply_semester_filter(self, filters, where, params):
        """
        Exact‐match filter on semester code ('F' or 'S').
        """
        val = filters.get("semester", "")
        if not isinstance(val, str) or not val.strip():
            return
        code = val.strip()[0].upper()
        col = self.fields['semester']['column']
        where.append(f"{col} = %s")
        params.append(code)
        self.logger.debug("Applied semester filter: %r", code)
