-- Phase 1: Batch-based invoicing foundation (non-breaking)

ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS is_invoiced BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS invoice_batch_id BIGINT NULL,
  ADD COLUMN IF NOT EXISTS invoice_id BIGINT NULL;

CREATE INDEX IF NOT EXISTS idx_transactions_is_invoiced
  ON transactions (is_invoiced);

CREATE INDEX IF NOT EXISTS idx_transactions_invoice_batch_id
  ON transactions (invoice_batch_id);

CREATE INDEX IF NOT EXISTS idx_transactions_invoice_id
  ON transactions (invoice_id);

CREATE TABLE IF NOT EXISTS invoice_batches (
  id BIGSERIAL PRIMARY KEY,
  batch_code TEXT UNIQUE NOT NULL,
  status TEXT NOT NULL DEFAULT 'DRAFT', -- DRAFT, PROCESSING, REVIEWED, INVOICED, CLOSED
  date_from DATE,
  date_to DATE,
  created_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_invoice_batches_status
  ON invoice_batches (status, created_at DESC);

CREATE TABLE IF NOT EXISTS invoice_batch_transactions (
  id BIGSERIAL PRIMARY KEY,
  invoice_batch_id BIGINT NOT NULL REFERENCES invoice_batches(id) ON DELETE CASCADE,
  transaction_id BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
  customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
  line_status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, READY, RATE_MISSING, MARKUP_MISSING, ERROR
  issue_note TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (invoice_batch_id, transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_invoice_batch_transactions_batch
  ON invoice_batch_transactions (invoice_batch_id, line_status);

CREATE INDEX IF NOT EXISTS idx_invoice_batch_transactions_customer
  ON invoice_batch_transactions (customer_id, invoice_batch_id);

-- Optional deferred FK on transactions.invoice_batch_id if table order varies between environments
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_transactions_invoice_batch_id'
  ) THEN
    ALTER TABLE transactions
      ADD CONSTRAINT fk_transactions_invoice_batch_id
      FOREIGN KEY (invoice_batch_id) REFERENCES invoice_batches(id) ON DELETE SET NULL;
  END IF;
EXCEPTION
  WHEN undefined_table THEN
    NULL;
END $$;

