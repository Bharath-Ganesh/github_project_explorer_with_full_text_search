# src/dao.py
import psycopg2
from psycopg2.extras import RealDictCursor
from project_utils.starter_class import build_context

class ProjectsDAO:
    def __init__(self):
        ctx = build_context(__name__)
        pg = ctx.get_required('postgres')
        self.table      = pg['table']
        self.fts_column = pg['fts_column']
        self.fields     = ctx.get_section('fields')

    def _connect(self):
        pg = build_context(__name__).get_required('postgres')
        return psycopg2.connect(
            dbname   = pg['dbname'],
            user     = pg['user'],
            password = pg['password'],
            host     = pg['host'],
            port     = pg['port'],
        )

    def search(self, filters: dict, select_aliases: list[str], limit: int):
        # 1) Build SELECT clause
        select_clause = ", ".join(
            f"{self.fields[a]['column']} AS {a}"
            for a in select_aliases if a in self.fields
        ) or "*"
        parts, where, params = [f"SELECT {select_clause} FROM {self.table}"], [], []

        # 2) Apply filters
        self._apply_author_filter(filters, where, params)
        self._apply_keyword_filter(filters, where, params)
        self._apply_library_filter(filters, where, params)
        self._apply_year_filter(filters, where, params)
        self._apply_semester_filter(filters, where, params)

        if where:
            parts.append("WHERE " + " AND ".join(where))

        # 3) ORDER BY
        if filters.get("_kw_clean"):
            parts.append(
                f"ORDER BY ts_rank({self.fts_column}, plainto_tsquery('english', %s)) DESC, created_at DESC"
            )
            params.append(filters["_kw_clean"])
        else:
            parts.append("ORDER BY created_at DESC")

        # 4) LIMIT
        parts.append("LIMIT %s")
        params.append(limit)

        # 5) Execute
        query = " ".join(parts)
        with self._connect() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            cur.close()
        return rows

    def ingest(self, project_dicts: list[dict]):
        insert_sql = """
             INSERT INTO projects
               (owner, repo, title, semester, team_members, repository_url,
                libraries, created_at, search_vector)
             VALUES (%(owner)s, %(repo)s, %(title)s, %(semester)s,
                     %(team_members)s, %(repository_url)s,
                     %(libraries)s, %(created_at)s,
                     to_tsvector('english',
                       coalesce(%(title)s,'') || ' ' || coalesce(%(readme_text)s,'')))
             ON CONFLICT (owner, repo) DO UPDATE
               SET title          = EXCLUDED.title,
                   semester       = EXCLUDED.semester,
                   team_members   = EXCLUDED.team_members,
                   repository_url = EXCLUDED.repository_url,
                   libraries      = EXCLUDED.libraries,
                   created_at     = EXCLUDED.created_at,
                   search_vector  = EXCLUDED.search_vector;
           """
        conn = self._connect()
        cur = conn.cursor()
        for proj in project_dicts:
            # … compute proj["semester"], etc. …
            cur.execute(insert_sql, proj)
        conn.commit()
        cur.close()
        conn.close()

    # ─── private filter builders ────────────────────────────────────────────────

    def _apply_author_filter(self, filters, where, params):
        val = filters.get("author")
        if not val:
            return
        col = self.fields.get('author', {}).get('field', 'team_members')
        where.append(f"to_tsvector('english', array_to_string({col}, ' ')) @@ plainto_tsquery('english', %s)")
        params.append(val)

    def _apply_keyword_filter(self, filters, where, params):
        kw = filters.get("keyword", "").strip()
        if not kw:
            return
        if kw.startswith('"') and kw.endswith('"'):
            op = "phraseto_tsquery"; clean = kw.strip('"')
        else:
            op = "plainto_tsquery";    clean = kw
        where.append(f"{self.fts_column} @@ {op}('english', %s)")
        params.append(clean)
        filters["_kw_clean"] = clean

    def _apply_library_filter(self, filters, where, params):
        val = filters.get("library") or filters.get("libraries") or ""
        if not val:
            return
        libs = val if isinstance(val, list) else [v.strip() for v in val.split(",") if v.strip()]
        if not libs:
            return
        col = self.fields['libraries']['column']
        where.append(f"{col} && %s::text[]")
        params.append(libs)

    def _apply_year_filter(self, filters, where, params):
        val = filters.get("year")
        if not val:
            return
        try:
            y = int(val)
        except ValueError:
            return
        col = self.fields.get('year', {}).get('field', 'year')  # default to 'year'
        where.append(f"{col} = %s")
        params.append(y)

    def _apply_semester_filter(self, filters, where, params):
        val = filters.get("semester","").strip()
        if not val:
            return
        code = val[0].upper()
        col = self.fields['semester']['column']
        where.append(f"{col} = %s")
        params.append(code)
