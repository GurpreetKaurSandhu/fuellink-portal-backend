CREATE TABLE IF NOT EXISTS billed_transaction_uploads (
  id BIGSERIAL PRIMARY KEY,
  uploaded_by_user_id INTEGER,
  original_filename TEXT NOT NULL,
  stored_filename TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS billed_transactions (
  id BIGSERIAL PRIMARY KEY,
  upload_id BIGINT REFERENCES billed_transaction_uploads(id) ON DELETE SET NULL,
  customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
  company_name TEXT,
  card_number TEXT,
  transaction_date DATE,
  location TEXT,
  city TEXT,
  province TEXT,
  document_number TEXT,
  product TEXT,
  volume_liters NUMERIC(14, 3),
  base_rate NUMERIC(14, 4),
  fet NUMERIC(14, 4),
  pft NUMERIC(14, 4),
  rate_per_ltr NUMERIC(14, 4),
  subtotal NUMERIC(14, 2),
  gst NUMERIC(14, 2),
  pst NUMERIC(14, 2),
  qst NUMERIC(14, 2),
  amount NUMERIC(14, 2),
  driver_name TEXT,
  invoice_number TEXT,
  invoice_date DATE,
  period_billed TEXT,
  raw_row JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_billed_transactions_customer ON billed_transactions(customer_id, transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_billed_transactions_invoice ON billed_transactions(invoice_number);
CREATE INDEX IF NOT EXISTS idx_billed_transactions_upload ON billed_transactions(upload_id);
