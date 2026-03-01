-- Phase 3: Generate invoices from reviewed batches with FL-YYYYMM-XXXX numbering

CREATE TABLE IF NOT EXISTS invoice_number_sequences (
  month_key CHAR(6) PRIMARY KEY, -- YYYYMM
  last_seq INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE customer_invoices
  ADD COLUMN IF NOT EXISTS due_date DATE;

-- Helpful index to find invoices by period/customer quickly
CREATE INDEX IF NOT EXISTS idx_customer_invoices_customer_period
  ON customer_invoices (customer_id, invoice_date DESC, id DESC);

