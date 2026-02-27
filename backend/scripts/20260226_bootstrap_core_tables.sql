CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  customer_number TEXT UNIQUE,
  company_name TEXT,
  rate_group_id INT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name TEXT,
  email TEXT UNIQUE,
  password TEXT,
  role TEXT,
  customer_id INT NULL REFERENCES customers(id) ON DELETE SET NULL,
  must_change_password BOOLEAN DEFAULT false,
  last_login_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS cards (
  id SERIAL PRIMARY KEY,
  customer_id INT NULL REFERENCES customers(id) ON DELETE SET NULL,
  card_number VARCHAR(20) UNIQUE NOT NULL,
  driver_name VARCHAR(150),
  status VARCHAR(20) DEFAULT 'active',
  created_at TIMESTAMPTZ DEFAULT now(),
  company_name TEXT,
  customer_number TEXT,
  source TEXT DEFAULT 'import',
  last_synced_at TIMESTAMPTZ,
  label TEXT
);

CREATE TABLE IF NOT EXISTS transactions (
  id BIGSERIAL PRIMARY KEY,
  customer_id INT NULL REFERENCES customers(id) ON DELETE SET NULL,
  card_number TEXT,
  purchase_datetime TIMESTAMPTZ,
  location TEXT,
  city TEXT,
  province TEXT,
  document_number TEXT,
  product TEXT,
  volume_liters NUMERIC,
  total_amount NUMERIC,
  created_at TIMESTAMPTZ DEFAULT now(),
  driver_name TEXT
);

CREATE TABLE IF NOT EXISTS support_requests (
  id BIGSERIAL PRIMARY KEY,
  customer_id INT NULL REFERENCES customers(id) ON DELETE SET NULL,
  user_id INT NULL REFERENCES users(id) ON DELETE SET NULL,
  type TEXT,
  card_number TEXT,
  message TEXT,
  status TEXT DEFAULT 'open',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS customer_invoices (
  id BIGSERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  invoice_no TEXT,
  invoice_date DATE,
  period_start DATE,
  period_end DATE,
  due_date DATE,
  total_amount NUMERIC,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS customer_invoice_files (
  id BIGSERIAL PRIMARY KEY,
  invoice_id BIGINT NOT NULL REFERENCES customer_invoices(id) ON DELETE CASCADE,
  file_type TEXT,
  original_filename TEXT,
  stored_filename TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_card_number ON transactions(card_number);
CREATE INDEX IF NOT EXISTS idx_cards_customer_id ON cards(customer_id);
CREATE INDEX IF NOT EXISTS idx_support_requests_customer_id ON support_requests(customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_invoices_customer_id ON customer_invoices(customer_id);
