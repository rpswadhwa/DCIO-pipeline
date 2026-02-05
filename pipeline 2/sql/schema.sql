CREATE TABLE IF NOT EXISTS plans (
  id INTEGER PRIMARY KEY,
  plan_name TEXT,
  sponsor TEXT,
  plan_year INTEGER,
  source_pdf TEXT
);

CREATE TABLE IF NOT EXISTS source_pages (
  id INTEGER PRIMARY KEY,
  plan_id INTEGER,
  page_number INTEGER,
  is_supplemental INTEGER,
  image_path TEXT,
  FOREIGN KEY(plan_id) REFERENCES plans(id)
);

CREATE TABLE IF NOT EXISTS investments (
  id INTEGER PRIMARY KEY,
  plan_id INTEGER,
  page_number INTEGER,
  row_id INTEGER,
  issuer_name TEXT,
  investment_description TEXT,
  asset_type TEXT,
  par_value TEXT,
  cost TEXT,
  current_value TEXT,
  units_or_shares TEXT,
  confidence REAL,
  FOREIGN KEY(plan_id) REFERENCES plans(id)
);

CREATE TABLE IF NOT EXISTS ocr_cells (
  id INTEGER PRIMARY KEY,
  page_number INTEGER,
  row_id INTEGER,
  cell_id INTEGER,
  bbox TEXT,
  text TEXT,
  confidence REAL
);
