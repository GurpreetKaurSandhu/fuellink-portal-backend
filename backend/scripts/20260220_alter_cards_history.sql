-- Add columns used by cards history logging
ALTER TABLE IF EXISTS cards_history
  ADD COLUMN IF NOT EXISTS status VARCHAR(20),
  ADD COLUMN IF NOT EXISTS driver_name VARCHAR(150),
  ADD COLUMN IF NOT EXISTS changed_by_user_id INTEGER REFERENCES users(id);

CREATE INDEX IF NOT EXISTS cards_history_changed_by_user_idx
  ON cards_history (changed_by_user_id);
