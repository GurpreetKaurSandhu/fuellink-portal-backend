CREATE TABLE IF NOT EXISTS customer_invoices (
  id bigserial PRIMARY KEY,
  customer_id int NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  invoice_no text NOT NULL,
  invoice_date date NOT NULL DEFAULT CURRENT_DATE,
  period_start date,
  period_end date,
  subtotal numeric(12,2) NOT NULL DEFAULT 0,
  gst numeric(12,2) NOT NULL DEFAULT 0,
  hst numeric(12,2) NOT NULL DEFAULT 0,
  pst numeric(12,2) NOT NULL DEFAULT 0,
  qst numeric(12,2) NOT NULL DEFAULT 0,
  total numeric(12,2) NOT NULL DEFAULT 0,
  totals_provided boolean NOT NULL DEFAULT false,
  currency text NOT NULL DEFAULT 'CAD',
  status text NOT NULL DEFAULT 'issued' CHECK (status IN ('draft', 'issued', 'paid', 'void')),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (customer_id, invoice_no)
);

CREATE TABLE IF NOT EXISTS customer_invoice_files (
  id bigserial PRIMARY KEY,
  invoice_id bigint NOT NULL REFERENCES customer_invoices(id) ON DELETE CASCADE,
  file_type text NOT NULL CHECK (file_type IN ('invoice_pdf', 'transaction_report_pdf')),
  original_name text,
  mime_type text,
  size_bytes bigint,
  storage_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (invoice_id, file_type)
);

CREATE INDEX IF NOT EXISTS customer_invoices_customer_date_idx
  ON customer_invoices (customer_id, invoice_date DESC);

CREATE INDEX IF NOT EXISTS customer_invoices_invoice_no_idx
  ON customer_invoices (invoice_no);
