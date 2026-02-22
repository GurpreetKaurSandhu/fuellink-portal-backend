CREATE TABLE IF NOT EXISTS pricing_import_lines (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER REFERENCES customers(id),
  card_number VARCHAR(20) NOT NULL,
  document_number TEXT,
  purchase_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  site_name TEXT,
  volume_liters NUMERIC(12,4),
  source_amount NUMERIC(12,4),
  source_ex_tax NUMERIC(12,4),
  source_in_tax NUMERIC(12,4),
  source_fet NUMERIC(12,4),
  source_pft NUMERIC(12,4),
  source_fct_pct NUMERIC(12,4),
  source_urban NUMERIC(12,4),
  source_gst NUMERIC(12,4),
  source_pst NUMERIC(12,4),
  source_qst NUMERIC(12,4),
  markup_per_liter NUMERIC(12,4),
  computed_ex_tax NUMERIC(12,4),
  computed_in_tax NUMERIC(12,4),
  transaction_id INTEGER REFERENCES transactions(id),
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS pricing_import_lines_customer_idx
  ON pricing_import_lines (customer_id, card_number, purchase_datetime);

CREATE INDEX IF NOT EXISTS pricing_import_lines_document_idx
  ON pricing_import_lines (document_number);

CREATE INDEX IF NOT EXISTS pricing_import_lines_transaction_idx
  ON pricing_import_lines (transaction_id);
