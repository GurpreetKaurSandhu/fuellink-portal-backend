-- Rates files table
CREATE TABLE IF NOT EXISTS rates_files (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers(id),
  original_filename TEXT NOT NULL,
  stored_filename TEXT NOT NULL,
  uploaded_by_user_id INTEGER REFERENCES users(id),
  effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Rates lines table
CREATE TABLE IF NOT EXISTS rates_lines (
  id SERIAL PRIMARY KEY,
  rates_file_id INTEGER REFERENCES rates_files(id) ON DELETE CASCADE,
  customer_id INTEGER NOT NULL REFERENCES customers(id),
  site_name TEXT NOT NULL,
  province VARCHAR(10) NOT NULL,
  price NUMERIC(10,4) NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS rates_files_customer_effective_idx
  ON rates_files (customer_id, effective_date);

CREATE INDEX IF NOT EXISTS rates_lines_customer_province_idx
  ON rates_lines (customer_id, province);
