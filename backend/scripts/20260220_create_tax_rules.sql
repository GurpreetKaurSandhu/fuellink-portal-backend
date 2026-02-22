-- Tax rules by province
CREATE TABLE IF NOT EXISTS tax_rules (
  province VARCHAR(10) NOT NULL,
  gst_rate NUMERIC(6,5) NOT NULL,
  pst_rate NUMERIC(6,5) NOT NULL,
  qst_rate NUMERIC(6,5) NOT NULL,
  effective_from DATE NOT NULL,
  PRIMARY KEY (province, effective_from)
);

-- Seed current default rates (GST is 5% federally)
INSERT INTO tax_rules (province, gst_rate, pst_rate, qst_rate, effective_from) VALUES
  -- GST only
  ('AB', 0.05, 0.00, 0.00, '2013-04-01'),
  ('NT', 0.05, 0.00, 0.00, '2013-04-01'),
  ('NU', 0.05, 0.00, 0.00, '2013-04-01'),
  ('YT', 0.05, 0.00, 0.00, '2013-04-01'),

  -- GST + PST
  ('BC', 0.05, 0.07, 0.00, '2013-04-01'),
  ('MB', 0.05, 0.07, 0.00, '2013-04-01'),
  ('SK', 0.05, 0.06, 0.00, '2013-04-01'),

  -- GST + QST (Quebec)
  ('QC', 0.05, 0.00, 0.09975, '2013-04-01'),

  -- HST provinces (recorded as GST + provincial component)
  ('ON', 0.05, 0.08, 0.00, '2013-04-01'),
  ('NB', 0.05, 0.10, 0.00, '2016-07-01'),
  ('NL', 0.05, 0.10, 0.00, '2016-07-01'),
  ('PE', 0.05, 0.10, 0.00, '2016-10-01'),

  -- Nova Scotia HST change (15% -> 14% on 2025-04-01)
  ('NS', 0.05, 0.10, 0.00, '2013-04-01'),
  ('NS', 0.05, 0.09, 0.00, '2025-04-01')
ON CONFLICT (province, effective_from) DO NOTHING;
