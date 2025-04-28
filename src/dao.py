# === src/dao.py ===
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from project_utils.starter_class import build_context


class ProjectsDAO:
    """
    Single DAO for both ingestion and querying.
    """
    def __init__(self):
        ctx = build_context(__name__)
        pg  = ctx.get_required_keys({"postgres"})["postgres"]
        # Connection params
        self.conn_params = {k: pg[k] for k in ("dbname","user","password","host","port")}
        self.table       = pg.get("table", "projects")
        self.fts_column  = pg.get("fts_column", "search_vector")
        self.fields      = ctx.get_fields()
        # DDL file path
        self.ddl_path    = Path(__file__).parent.parent / 'project_utils' / 'postgres_schema' / 'projects_table.sql'

    def _connect(self):
        return psycopg2.connect(**self.conn_params)

    def _select_clause(self, aliases):
        return ", ".join(f"{self.fields[a]['column']} AS {a}" for a in aliases)

    def _drop_and_create(self, cur):
        cur.execute(f"DROP TABLE IF EXISTS {self.table} CASCADE;")
        ddl = self.ddl_path.read_text()
        for stmt in ddl.split(';'):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt + ';')

    def ingest(self, records: list[dict]):
        # Build insert SQL
        aliases = list(self.fields.keys())
        cols    = [self.fields[a]['column'] for a in aliases]
        col_list= ", ".join(cols)
        ph      = ", ".join(["%s"] * len(cols))
        sql     = f"INSERT INTO {self.table} ({col_list}) VALUES ({ph});"
        with self._connect() as conn:
            cur = conn.cursor()
            self._drop_and_create(cur)
            conn.commit()
            for rec in records:
                params = [rec.get(a) for a in aliases]
                cur.execute(sql, params)
            conn.commit()
            cur.close()

    def search(self, filters: dict, select_aliases: list[str], limit: int):
        select_clause = self._select_clause(select_aliases)
        parts, params = [f"SELECT {select_clause} FROM {self.table}"], []
        where = []
        # Full-text
        kw = filters.get("keyword","").strip()
        if kw:
            op = "phraseto_tsquery" if kw.startswith('"') and kw.endswith('"') else "plainto_tsquery"
            kwc= kw.strip('"')
            where.append(f"{self.fts_column} @@ {op}('english', %s)")
            params.append(kwc)
        # Other filters
        for alias, val in filters.items():
            if alias=='keyword' or not val:
                continue
            col = self.fields.get(alias,{}).get('column', alias)
            if alias=='libraries':
                libs = [x.strip() for x in val.split(',')]
                where.append(f"{col} && %s::text[]")
                params.append(libs)
            else:
                where.append(f"{col} ILIKE %s")
                params.append(f"%{val}%")
        if where:
            parts.append("WHERE " + " AND ".join(where))
        # Order
        if kw:
            parts.append(f"ORDER BY ts_rank({self.fts_column}, plainto_tsquery('english', %s)) DESC, created_at DESC")
            params.append(kwc)
        else:
            parts.append("ORDER BY created_at DESC")
        parts.append("LIMIT %s"); params.append(limit)
        query = " ".join(parts)
        with self._connect() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            cur.close()
        return rows