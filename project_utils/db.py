# project_utils/db.py

import psycopg2
from psycopg2.extras import RealDictCursor
from project_utils.starter_class import build_context


class ProjectsDAO:
    def __init__(self):
        # --- load everything from config.yaml ---
        cfg = build_context(__name__)
        pg = cfg.get_required_keys({'postgres'})['postgres']

        # connection params
        self.conn_params = {
            'dbname':   pg['dbname'],
            'user':     pg['user'],
            'password': pg['password'],
            'host':     pg['host'],
            'port':     pg['port'],
        }

        # table + FTS settings
        self.table      = pg.get('table', 'projects')
        self.fts_column = pg.get('fts_column', 'search_vector')

        # field-to-column mappings
        self.fields = cfg.get_required_keys({'fields'})['fields']

        # grab a few often-used raw column names from the same mapping
        self.created_at   = self.fields['created_at']['column']
        self.team_members = self.fields['team_members']['column']
        self.libraries    = self.fields['libraries']['column']
        self.semester     = self.fields['semester']['column']

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)


    def _select_clause(self):
        # build "col AS alias" for every configured field
        return ", ".join(
            f"{col_cfg['column']} AS {alias}"
            for alias, col_cfg in self.fields.items()
        )

    def search_by_keyword(self, keyword: str, limit: int = 50):
        select = self._select_clause()
        sql = f"""
        SELECT {select},
               ts_rank({self.fts_column}, plainto_tsquery('english', %s)) AS rank
          FROM {self.table}
         WHERE {self.fts_column} @@ plainto_tsquery('english', %s)
         ORDER BY rank DESC, {self.created_at} DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (keyword, keyword, limit))
            return cur.fetchall()

    def find_by_member(self, member: str, limit: int = 50):
        select = self._select_clause()
        sql = f"""
        SELECT {select}
          FROM {self.table}
         WHERE {self.team_members} @> ARRAY[%s]
         ORDER BY {self.created_at} DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (member, limit))
            return cur.fetchall()

    def search_phrase(self, phrase: str, limit: int = 50):
        select = self._select_clause()
        sql = f"""
        SELECT {select}
          FROM {self.table}
         WHERE {self.fts_column} @@ phraseto_tsquery('english', %s)
         ORDER BY {self.created_at} DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (phrase, limit))
            return cur.fetchall()

    def search_by_libraries(self, libs: list[str], limit: int = 50):
        select = self._select_clause()
        sql = f"""
        SELECT {select}
          FROM {self.table}
         WHERE {self.libraries} && %s
         ORDER BY {self.created_at} DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (libs, limit))
            return cur.fetchall()

    def search_by_member_and_keyword(self, member: str, keyword: str, limit: int = 50):
        select = self._select_clause()
        sql = f"""
        SELECT {select},
               ts_rank({self.fts_column}, plainto_tsquery('english', %s)) AS rank
          FROM {self.table}
         WHERE {self.team_members} @> ARRAY[%s]
           AND {self.fts_column} @@ plainto_tsquery('english', %s)
         ORDER BY rank DESC, {self.created_at} DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (keyword, member, keyword, limit))
            return cur.fetchall()

    def top_recent_by_keyword(self, keyword: str, limit: int = 10):
        select = self._select_clause()
        sql = f"""
        SELECT {select},
               ts_rank({self.fts_column}, plainto_tsquery('english', %s)) AS rank
          FROM {self.table}
         WHERE {self.fts_column} @@ plainto_tsquery('english', %s)
         ORDER BY {self.created_at} DESC, rank DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (keyword, keyword, limit))
            return cur.fetchall()

    def filter_by_semester(self, semester: str, limit: int = 50):
        select = self._select_clause()
        sql = f"""
        SELECT {select}
          FROM {self.table}
         WHERE {self.semester} = %s
         ORDER BY {self.created_at} DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (semester, limit))
            return cur.fetchall()

    def search_in_semester(self, semester: str, keyword: str, limit: int = 50):
        select = self._select_clause()
        sql = f"""
        SELECT {select},
               ts_rank({self.fts_column}, plainto_tsquery('english', %s)) AS rank
          FROM {self.table}
         WHERE {self.semester} = %s
           AND {self.fts_column} @@ plainto_tsquery('english', %s)
         ORDER BY rank DESC, {self.created_at} DESC
         LIMIT %s;
        """
        with self.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (keyword, semester, keyword, limit))
            return cur.fetchall()

# project_utils/db.py

# … your ProjectsDAO class above …

def get_connection():
    """
    Backwards-compatible free-function for fetching a psycopg2 connection.
    """
    return ProjectsDAO().get_connection()