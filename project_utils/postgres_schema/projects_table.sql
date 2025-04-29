-- drop old table if it exists
DROP TABLE IF EXISTS projects CASCADE;

-- drop the old readme_text column
CREATE TABLE projects (
  id             SERIAL      PRIMARY KEY,
  owner          TEXT        NOT NULL,
  repo           TEXT        NOT NULL,
  title          TEXT,                -- from JSON‐extracted title
  year           INT,                 -- 2017, 2018, …
  semester       TEXT,             -- 'F' or 'S'
  team_members   TEXT[],              -- GitHub logins
  repository_url TEXT,
  libraries      TEXT[],              -- detected imports
  created_at     TIMESTAMPTZ,         -- fork creation timestamp
  search_vector  TSVECTOR,
  UNIQUE(owner, repo)       -- full‐text index

);

-- full-text search on your blob
CREATE INDEX idx_projects_fts
  ON projects USING GIN (search_vector);

-- fast array‐overlap for libraries
CREATE INDEX idx_projects_libraries
  ON projects USING GIN (libraries);

-- fast exact match on year
CREATE INDEX idx_projects_year
  ON projects (year);
