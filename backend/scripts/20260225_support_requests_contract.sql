CREATE TABLE IF NOT EXISTS support_requests (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  card_id INTEGER REFERENCES cards(id) ON DELETE SET NULL,
  card_number TEXT,
  request_type TEXT NOT NULL,
  message TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  internal_notes TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

ALTER TABLE support_requests
  ADD COLUMN IF NOT EXISTS card_id INTEGER REFERENCES cards(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS card_number TEXT,
  ADD COLUMN IF NOT EXISTS request_type TEXT,
  ADD COLUMN IF NOT EXISTS message TEXT,
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open',
  ADD COLUMN IF NOT EXISTS internal_notes TEXT,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_support_requests_customer_id
  ON support_requests(customer_id);

CREATE INDEX IF NOT EXISTS idx_support_requests_status
  ON support_requests(status);
