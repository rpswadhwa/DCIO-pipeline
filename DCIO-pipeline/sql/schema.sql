CREATE TABLE IF NOT EXISTS plans (
  id INTEGER PRIMARY KEY,
  sponsor_ein TEXT UNIQUE NOT NULL,
  plan_name TEXT,
  plan_number TEXT,
  sponsor TEXT,
  administrator_name TEXT,
  administrator_address TEXT,
  plan_type TEXT,
  plan_year INTEGER,
  plan_year_begin DATE,
  plan_year_end DATE,
  total_participants_bol INTEGER,
  total_participants_eol INTEGER,
  total_assets_bol REAL,
  total_assets_eol REAL,
  total_liabilities_bol REAL,
  total_liabilities_eol REAL,
  source_pdf TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
  sponsor_ein TEXT NOT NULL,
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
  FOREIGN KEY(sponsor_ein) REFERENCES plans(sponsor_ein)
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
