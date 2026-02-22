-- Enable pg_trgm for similarity matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Extract province code from location text
CREATE OR REPLACE FUNCTION extract_province_from_location(loc text)
RETURNS VARCHAR AS $$
DECLARE
  prov VARCHAR;
BEGIN
  IF loc IS NULL THEN
    RETURN NULL;
  END IF;

  SELECT UPPER(m[1])
  INTO prov
  FROM regexp_matches(loc, '\\m(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\\M', 'i') AS m
  LIMIT 1;

  RETURN prov;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Match a rates_lines row for a transaction context
CREATE OR REPLACE FUNCTION rate_match(
  p_customer_id INTEGER,
  p_province VARCHAR,
  p_location TEXT,
  p_purchase_datetime TIMESTAMP WITHOUT TIME ZONE
)
RETURNS TABLE(
  rates_line_id INTEGER,
  rates_file_id INTEGER,
  customer_id INTEGER,
  site_name TEXT,
  province VARCHAR,
  price NUMERIC(10,4),
  effective_date DATE
) AS $$
DECLARE
  v_date DATE;
BEGIN
  v_date := COALESCE(p_purchase_datetime::date, CURRENT_DATE);

  RETURN QUERY
  WITH latest_date AS (
    SELECT rf.effective_date
    FROM rates_files rf
    WHERE rf.customer_id = p_customer_id
      AND rf.effective_date <= v_date
    ORDER BY rf.effective_date DESC, rf.id DESC
    LIMIT 1
  ),
  fallback_date AS (
    SELECT rf.effective_date
    FROM rates_files rf
    WHERE rf.customer_id = p_customer_id
    ORDER BY rf.effective_date DESC, rf.id DESC
    LIMIT 1
  ),
  chosen_date AS (
    SELECT COALESCE(
      (SELECT effective_date FROM latest_date),
      (SELECT effective_date FROM fallback_date)
    ) AS effective_date
  ),
  candidates AS (
    SELECT rl.*, rf.effective_date
    FROM rates_lines rl
    JOIN rates_files rf ON rf.id = rl.rates_file_id
    JOIN chosen_date cd ON cd.effective_date = rf.effective_date
    WHERE rl.customer_id = p_customer_id
      AND (
        p_province IS NULL OR p_province = '' OR rl.province ILIKE p_province
      )
  )
  SELECT c.id, c.rates_file_id, c.customer_id, c.site_name, c.province, c.price, c.effective_date
  FROM (
    SELECT
      c.*,
      CASE
        WHEN c.site_name = p_location THEN 0
        WHEN c.site_name ILIKE p_location THEN 1
        WHEN p_location ILIKE '%' || c.site_name || '%' THEN 2
        ELSE 3
      END AS match_rank,
      similarity(c.site_name, COALESCE(p_location, '')) AS sim
    FROM candidates c
  ) c
  ORDER BY c.match_rank ASC, c.sim DESC, c.id ASC
  LIMIT 1;
END;
$$ LANGUAGE plpgsql;
