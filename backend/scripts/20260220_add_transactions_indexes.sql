-- Enable pg_trgm for ILIKE search optimization
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Composite index for customer + date filtering/sorting
CREATE INDEX IF NOT EXISTS transactions_customer_date_idx
  ON transactions (customer_id, purchase_datetime DESC);

-- Lookup by card number
CREATE INDEX IF NOT EXISTS transactions_card_number_idx
  ON transactions (card_number);

-- Trigram index for driver_name ILIKE
CREATE INDEX IF NOT EXISTS transactions_driver_name_trgm_idx
  ON transactions USING GIN (driver_name gin_trgm_ops);
