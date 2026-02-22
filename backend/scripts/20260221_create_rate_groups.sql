-- Rate groups
CREATE TABLE IF NOT EXISTS rate_groups (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) UNIQUE NOT NULL,
  markup_per_liter NUMERIC(10,4) NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

ALTER TABLE customers
  ADD COLUMN IF NOT EXISTS rate_group_id INTEGER REFERENCES rate_groups(id);

-- Seed groups
INSERT INTO rate_groups (name, markup_per_liter) VALUES
  ('A', 0.00),
  ('B', 0.01),
  ('C', 0.03),
  ('D', 0.04)
ON CONFLICT (name) DO NOTHING;
