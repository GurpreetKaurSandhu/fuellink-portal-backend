-- Add source and computed fields for SuperPass import reporting
ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS computed_ex_tax NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS computed_in_tax NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_ex_tax NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_in_tax NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_fet NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_pft NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_fct_pct NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_urban NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_gst NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_pst NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_qst NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS source_amount NUMERIC(12,4);
