-- Patch existing customer_invoices + customer_invoice_files to match code expectations

-- 1) customer_invoices: add missing columns
ALTER TABLE public.customer_invoices
  ADD COLUMN IF NOT EXISTS totals_provided boolean NOT NULL DEFAULT false;

ALTER TABLE public.customer_invoices
  ALTER COLUMN invoice_date SET DEFAULT CURRENT_DATE;

-- 2) Backfill totals_provided (safe default)
UPDATE public.customer_invoices
SET totals_provided =
  (COALESCE(subtotal,0) <> 0 OR COALESCE(gst,0) <> 0 OR COALESCE(hst,0) <> 0 OR
   COALESCE(pst,0) <> 0 OR COALESCE(qst,0) <> 0 OR COALESCE(total,0) <> 0)
WHERE totals_provided = false;

-- 3) Ensure UNIQUE(customer_id, invoice_no) exists for ON CONFLICT
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_customer_invoices_customer_invoice_no'
  ) THEN
    ALTER TABLE public.customer_invoices
      ADD CONSTRAINT uq_customer_invoices_customer_invoice_no
      UNIQUE (customer_id, invoice_no);
  END IF;
END $$;

-- 4) Ensure UNIQUE(invoice_id, file_type) exists for invoice files upsert
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_customer_invoice_files_invoice_filetype'
  ) THEN
    ALTER TABLE public.customer_invoice_files
      ADD CONSTRAINT uq_customer_invoice_files_invoice_filetype
      UNIQUE (invoice_id, file_type);
  END IF;
END $$;

-- 5) Helpful indexes
CREATE INDEX IF NOT EXISTS customer_invoices_customer_date_idx
  ON public.customer_invoices (customer_id, invoice_date DESC);

CREATE INDEX IF NOT EXISTS customer_invoices_invoice_no_idx
  ON public.customer_invoices (invoice_no);
