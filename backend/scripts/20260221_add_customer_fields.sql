-- Add missing columns to customers table for customer import + admin screens

ALTER TABLE customers
  ADD COLUMN IF NOT EXISTS customer_number TEXT,
  ADD COLUMN IF NOT EXISTS company_name TEXT,
  ADD COLUMN IF NOT EXISTS owner_name TEXT,
  ADD COLUMN IF NOT EXISTS city TEXT,
  ADD COLUMN IF NOT EXISTS email TEXT,
  ADD COLUMN IF NOT EXISTS bv TEXT,
  ADD COLUMN IF NOT EXISTS fuellink_card INTEGER,
  ADD COLUMN IF NOT EXISTS otp_setup TEXT,
  ADD COLUMN IF NOT EXISTS deposit NUMERIC(12,2),
  ADD COLUMN IF NOT EXISTS address TEXT,
  ADD COLUMN IF NOT EXISTS security_deposit_invoice TEXT,
  ADD COLUMN IF NOT EXISTS customer_status BOOLEAN DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS reference_name TEXT,
  ADD COLUMN IF NOT EXISTS comment TEXT,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();

-- Unique index so ON CONFLICT(customer_number) works
CREATE UNIQUE INDEX IF NOT EXISTS customers_customer_number_uidx
  ON customers (customer_number);

-- If your old table had "name", copy it into company_name (safe if name column exists)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='customers' AND column_name='name'
  ) THEN
    UPDATE customers
    SET company_name = COALESCE(company_name, name)
    WHERE company_name IS NULL AND name IS NOT NULL;
  END IF;
END $$;
