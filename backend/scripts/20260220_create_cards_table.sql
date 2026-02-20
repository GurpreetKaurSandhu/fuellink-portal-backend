-- Cards table
CREATE TABLE IF NOT EXISTS cards (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER REFERENCES customers(id),
  card_number VARCHAR(20) UNIQUE NOT NULL,
  driver_name VARCHAR(150),
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE UNIQUE INDEX IF NOT EXISTS cards_card_number_uidx
  ON cards (card_number);

CREATE INDEX IF NOT EXISTS cards_customer_card_idx
  ON cards (customer_id, card_number);

-- Optional history table for status/driver changes
CREATE TABLE IF NOT EXISTS cards_history (
  id SERIAL PRIMARY KEY,
  card_id INTEGER REFERENCES cards(id) ON DELETE CASCADE,
  customer_id INTEGER REFERENCES customers(id),
  card_number VARCHAR(20) NOT NULL,
  old_status VARCHAR(20),
  new_status VARCHAR(20),
  old_driver_name VARCHAR(150),
  new_driver_name VARCHAR(150),
  changed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cards_history_card_id_idx
  ON cards_history (card_id);

CREATE INDEX IF NOT EXISTS cards_history_customer_card_idx
  ON cards_history (customer_id, card_number);
