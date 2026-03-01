-- Phase 2: Invoice processing calculations, flags, and customer markup rules

CREATE TABLE IF NOT EXISTS customer_markup_rules (
  id BIGSERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  product TEXT,
  province TEXT,
  site TEXT,
  markup_type TEXT NOT NULL CHECK (markup_type IN ('per_liter', 'percent')),
  markup_value NUMERIC(12,6) NOT NULL,
  priority INTEGER NOT NULL DEFAULT 100,
  is_active BOOLEAN NOT NULL DEFAULT true,
  effective_from DATE,
  effective_to DATE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_markup_rules_lookup
  ON customer_markup_rules (customer_id, is_active, priority, effective_from, effective_to);

ALTER TABLE invoice_batches
  ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reviewed_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;

ALTER TABLE invoice_batch_transactions
  ADD COLUMN IF NOT EXISTS rate_group_id INTEGER,
  ADD COLUMN IF NOT EXISTS rate_group_name TEXT,
  ADD COLUMN IF NOT EXISTS rate_source_effective_date DATE,
  ADD COLUMN IF NOT EXISTS base_rate NUMERIC(12,6),
  ADD COLUMN IF NOT EXISTS markup_rule_id BIGINT,
  ADD COLUMN IF NOT EXISTS markup_rule_used TEXT,
  ADD COLUMN IF NOT EXISTS markup_type TEXT,
  ADD COLUMN IF NOT EXISTS markup_value NUMERIC(12,6),
  ADD COLUMN IF NOT EXISTS rate_per_ltr NUMERIC(12,6),
  ADD COLUMN IF NOT EXISTS subtotal NUMERIC(14,4),
  ADD COLUMN IF NOT EXISTS gst NUMERIC(14,4),
  ADD COLUMN IF NOT EXISTS pst NUMERIC(14,4),
  ADD COLUMN IF NOT EXISTS qst NUMERIC(14,4),
  ADD COLUMN IF NOT EXISTS amount_total NUMERIC(14,4),
  ADD COLUMN IF NOT EXISTS flags TEXT[] DEFAULT ARRAY[]::TEXT[];

CREATE INDEX IF NOT EXISTS idx_invoice_batch_transactions_status
  ON invoice_batch_transactions (invoice_batch_id, line_status, customer_id);

