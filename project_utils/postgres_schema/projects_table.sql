-- drop the old readme_text column
CREATE TABLE projects (
  id            SERIAL       PRIMARY KEY,
  owner         TEXT         NOT NULL,
  repo          TEXT         NOT NULL,
  title         TEXT,            -- now populated from JSON‐extracted title
  semester      TEXT,
  team_members  TEXT[],          -- array of GitHub logins
  repository_url TEXT,
  libraries     TEXT[],          -- detected imports
  created_at    TIMESTAMPTZ,     -- fork creation timestamp
  search_vector TSVECTOR        -- full‐text index
);

CREATE INDEX idx_projects_fts
  ON projects USING GIN (search_vector);
