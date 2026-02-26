CREATE TABLE IF NOT EXISTS transaction_uploads (
  id BIGSERIAL PRIMARY KEY,
  uploaded_by_user_id INT,
  original_filename TEXT,
  stored_filename TEXT,
  source_type TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  parse_status TEXT,
  parse_error TEXT,
  rows_inserted INT DEFAULT 0,
  rows_skipped INT DEFAULT 0,
  rows_unmatched INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_transaction_uploads_created_at
  ON transaction_uploads(created_at DESC);
