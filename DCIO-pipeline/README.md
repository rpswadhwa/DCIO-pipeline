# DCIO Pipeline: Form 5500 Investment Data Extraction & Cleaning

A comprehensive data extraction pipeline that processes SEC Form 5500 pension plan documents to extract, validate, clean, and structure investment holdings data.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Libraries & Dependencies](#libraries--dependencies)
- [Pipeline Stages](#pipeline-stages)
- [Data Schema](#data-schema)
- [Configuration](#configuration)
- [Usage](#usage)
- [Output Files](#output-files)
- [AWS Deployment](#aws-deployment)

---

## 🎯 Overview

The DCIO Pipeline extracts investment holdings information from Form 5500 Schedule H (Line 4i) supplemental schedules. It processes PDF documents containing tabular investment data, performs OCR when needed, maps data to a structured schema, and applies comprehensive cleaning logic to produce high-quality investment records.

### Key Features

- **Multi-mode extraction**: Text-based (fast) or OCR-based (comprehensive)
- **Intelligent page classification**: Identifies supplemental investment schedules
- **Table detection**: Locates and extracts tabular data from complex layouts
- **LLM-powered mapping**: Uses OpenAI GPT models for intelligent column mapping
- **Comprehensive data cleaning**: Removes totals, metadata, duplicates, and handles split rows
- **Database persistence**: SQLite storage with full relational schema
- **Asset type enhancement**: Automatically populates missing asset classifications

---

## 🏗️ Architecture

### Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INPUT: Form 5500 PDFs                              │
│                       (data/inputs/raw/*.pdf by default)                     │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 1: DOCUMENT INGESTION & PAGE CLASSIFICATION                          │
│  ─────────────────────────────────────────────────────────                  │
│  • PDF → Images (pdf2image) [if OCR mode]                                   │
│  • Text extraction (pdfplumber) [if text mode]                              │
│  • Keyword-based page classification                                        │
│  • Identify supplemental investment schedules (Schedule H Line 4i)          │
│                                                                              │
│  Modules: src/ingest.py, src/classify_pages.py, src/text_extract.py        │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 2: TABLE DETECTION & EXTRACTION                                      │
│  ────────────────────────────────────────────                               │
│  • Image normalization (contrast enhancement, deskewing)                    │
│  • Table region detection (PaddleOCR Structure)                             │
│  • Cell boundary detection (OpenCV morphological operations)                │
│  • Table structure extraction (Camelot-py)                                  │
│                                                                              │
│  Modules: src/normalize_images.py, src/detect_tables.py                    │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 3: OCR & TEXT RECOGNITION                                            │
│  ──────────────────────────────────                                         │
│  • Cell-level OCR (PaddleOCR)                                               │
│  • Confidence scoring                                                       │
│  • Text normalization (whitespace, encoding)                                │
│  • Row sorting and alignment                                                │
│                                                                              │
│  Modules: src/ocr_passes.py                                                 │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 4: INTELLIGENT COLUMN MAPPING                                        │
│  ──────────────────────────────────                                         │
│  • Header detection and extraction                                          │
│  • Fuzzy matching against schema (RapidFuzz)                                │
│  • LLM-enhanced mapping (OpenAI GPT-4) [optional]                           │
│  • Map table columns to schema fields                                       │
│  • Extract plan identifiers (EIN, plan number)                              │
│                                                                              │
│  Modules: src/llm_map.py, src/text_extract.py                              │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 5: DATA VALIDATION & EXPORT                                          │
│  ────────────────────────────────────                                       │
│  • Schema validation                                                        │
│  • Required field checks                                                    │
│  • Numeric field verification                                               │
│  • Export to raw CSV                                                        │
│                                                                              │
│  Modules: src/validate.py, src/export_csv.py                               │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 6: COMPREHENSIVE DATA CLEANING                                       │
│  ───────────────────────────────────────                                    │
│  • Handle split rows (values on separate lines)                             │
│  • Extract asset types from issuer names                                    │
│  • Remove total/summary rows with pattern matching                          │
│  • Remove metadata and header rows                                          │
│  • Deduplicate investment records                                           │
│  • Preserve participant loans for QA                                        │
│                                                                              │
│  Modules: src/data_cleaner.py, cleanup_with_dedup.py                       │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 7: ASSET TYPE ENHANCEMENT                                            │
│  ──────────────────────────────────                                         │
│  • Analyze investment descriptions                                          │
│  • Pattern matching for asset classifications                               │
│  • Populate missing asset_type fields                                       │
│                                                                              │
│  Modules: enhance_asset_types.py                                            │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 8: DATABASE PERSISTENCE                                              │
│  ────────────────────────────────                                          │
│  • Create SQLite database with relational schema                            │
│  • Load plan metadata (EIN, plan number)                                    │
│  • Store investment holdings                                                │
│  • Link source pages to plans                                               │
│                                                                              │
│  Modules: src/load_db.py                                                    │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 9: PLAN DETAILS EXTRACTION                                           │
│  ───────────────────────────────────                                        │
│  • Extract plan name from Form 5500 Part II-1a                              │
│  • Extract sponsor name from Schedule H or EIN line                         │
│  • Update plans table with extracted metadata                               │
│  • Fallback to filename-based inference if needed                           │
│                                                                              │
│  Modules: extract_plan_details.py                                           │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 10: INVESTMENT DATA STANDARDIZATION                                  │
│  ────────────────────────────────────────                                   │
│  • Separate issuer names from investment descriptions                       │
│  • Standardize asset manager names (Vanguard, BlackRock, etc.)             │
│  • Expand abbreviations (INST→Institutional, IDX→Index)                     │
│  • Clean up fund names and share classes                                    │
│  • Restore missing descriptions from raw data                               │
│                                                                              │
│  Modules: cleanup_investment_names.py, restore_missing_descriptions.py     │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 11: LLM-ENHANCED QUALITY CONTROL                                     │
│  ─────────────────────────────────────────                                  │
│  • AI-powered review using OpenAI GPT-4o-mini                               │
│  • Context-aware improvements beyond rule-based logic                       │
│  • Abbreviation expansion and name standardization                          │
│  • Batch processing with rate limiting                                      │
│  • Comprehensive data quality validation                                    │
│                                                                              │
│  Modules: llm_enhance_investments.py                                        │
└────────────────────────┬────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        OUTPUT FILES                                          │
│                                                                              │
│  ✓ investments_raw.csv       - Unprocessed extraction results               │
│  ✓ investments_clean.csv     - LLM-enhanced, production-ready data          │
│  ✓ removed_total_rows.csv    - Removed totals for verification             │
│  ✓ pipeline.db               - SQLite database with complete metadata       │
│  ✓ qa_report.json            - Validation quality report                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📚 Libraries & Dependencies

### Core Data Processing
- **pdfplumber** (0.11.4) - PDF text extraction and table detection
- **camelot-py** (0.11.0) - Advanced table extraction from PDFs
- **pandas** (2.2.1) - Data manipulation and analysis
- **numpy** (1.26.4) - Numerical operations

### OCR & Computer Vision
- **PaddleOCR** - Text detection and recognition (imported dynamically)
- **pdf2image** (1.16.3) - PDF to image conversion
- **Pillow** (>=11.0.0) - Image processing
- **OpenCV** (cv2) - Computer vision operations for table detection
- **ghostscript** (0.7) - PDF rendering backend

### AI/ML
- **openai** (1.50.2) - GPT-4 integration for intelligent column mapping
- **rapidfuzz** (3.6.1) - Fast fuzzy string matching for header mapping

### Database & Configuration
- **sqlalchemy** (2.0.28) - Database ORM and schema management
- **pydantic** (>=2.7.0) - Data validation and settings management
- **pyyaml** (6.0.2) - Configuration file parsing

### Utilities
- **python-dotenv** (1.0.1) - Environment variable management

Install all dependencies:
```bash
pip install -r requirements.txt
```

---

## 🔄 Pipeline Stages

### Stage 1: Document Ingestion & Classification

**Module**: `src/ingest.py`, `src/classify_pages.py`, `src/text_extract.py`

**Purpose**: Identify which pages of Form 5500 PDFs contain investment schedule data.

**Process**:
1. Convert PDF pages to images (350 DPI) if OCR mode enabled
2. Extract text from first 12 lines of each page
3. Match against keywords from `config/keywords.yml`:
   - Positive keywords: "ASSETS", "INVESTMENTS", "SCHEDULE H", etc.
   - Negative keywords: "TRANSACTIONS", "LOANS OUTSTANDING", etc.
4. Mark pages with `is_supplemental=1` if criteria met

**Key Functions**:
- `classify_pages_text()` - Text-based classification (fast)
- `classify_pages()` - OCR-based classification (comprehensive)
- `extract_ein_from_pdf()` - Extract plan identifiers

---

### Stage 2: Table Detection & Extraction

**Module**: `src/detect_tables.py`, `src/normalize_images.py`

**Purpose**: Locate and extract table structures from document images.

**Process**:
1. **Image Normalization**:
   - Convert to grayscale
   - Apply contrast enhancement (CLAHE)
   - Deskew rotated images
   - Border detection and cropping

2. **Table Region Detection**:
   - Use PaddleOCR PPStructure for table localization
   - Fallback to full page if no tables detected

3. **Cell Detection**:
   - Apply adaptive thresholding
   - Detect horizontal/vertical lines using morphological operations
   - Extract cell bounding boxes
   - Sort cells by row and column position

**Key Functions**:
- `normalize_pages()` - Image preprocessing
- `detect_tables()` - Table and cell detection
- `_find_cells()` - OpenCV-based cell extraction

---

### Stage 3: OCR & Text Recognition

**Module**: `src/ocr_passes.py`

**Purpose**: Extract text content from detected table cells.

**Process**:
1. Run PaddleOCR on each detected cell region
2. Execute two OCR passes:
   - Table-optimized OCR
   - Text-optimized OCR
3. Select result with higher confidence and length
4. Normalize whitespace and special characters
5. Store text with confidence scores

**Key Functions**:
- `run_ocr()` - Multi-pass OCR execution
- `_ocr_cell()` - Single cell OCR with confidence

---

### Stage 4: Intelligent Column Mapping

**Module**: `src/llm_map.py`, `src/text_extract.py`

**Purpose**: Map table columns to schema fields.

**Process**:
1. **Header Detection**:
   - Scan first rows for header keywords
   - Identify header row position

2. **Fuzzy Matching**:
   - Use RapidFuzz to match headers against `config/schema.yml` synonyms
   - Score threshold: 70% similarity

3. **LLM Enhancement** (optional):
   - Send headers to GPT-4 for intelligent mapping
   - Handle ambiguous or non-standard column names

4. **Data Extraction**:
   - Extract investment rows using Camelot
   - Handle multi-page tables
   - Preserve row structure and relationships

**Key Functions**:
- `map_rows_with_llm()` - Column mapping orchestration
- `_best_header_match()` - Fuzzy header matching
- `extract_tables_and_map()` - Complete extraction and mapping

---

### Stage 5: Data Validation

**Module**: `src/validate.py`

**Purpose**: Ensure data quality and completeness.

**Validation Checks**:
- Required fields present (issuer_name, current_value)
- Numeric fields contain valid numbers
- Confidence thresholds met (75% default, 60% low threshold)
- Currency format validation (max 32 chars)
- Negative value handling

**Output**: `qa_report.json` with validation statistics

---

### Stage 6: Comprehensive Data Cleaning

**Module**: `src/data_cleaner.py`, `cleanup_with_dedup.py`

**Purpose**: Remove noise and normalize investment data.

#### 6.1 Handle Split Rows
```python
handle_split_rows(df)
```
Merges rows where investment values appear on separate lines from issuer names.

#### 6.2 Parse Investment Rows
```python
parse_investment_row(row)
```
- Extracts asset type from issuer names
- Separates issuer from description
- Patterns: "Mutual Fund", "Common Stock", "ETF", etc.

#### 6.3 Remove Total Rows
```python
remove_total_rows(rows, verbose=True)
```
**Indicators**:
- Exact match: "Total"
- Phrases: "total investments", "grand total", "subtotal"
- Pattern: Rows starting with "Total [Manager] [Asset Type]"

**Preservation**:
- Fund names like "PIMCO Total Return"
- Funds with company names: "Vanguard Total Bond Market"

**Logic**:
```
IF issuer == "Total" OR contains total_indicator:
    → Remove
ELIF starts_with "Total" AND has_fund_pattern:
    → Preserve (legitimate fund)
ELIF starts_with "Total" AND (short_abbreviation OR no_details):
    → Remove (subtotal marker)
```

#### 6.4 Remove Metadata Rows
```python
remove_metadata_rows(rows, preserve_loans=True)
```
**Excluded Keywords**:
- "form 5500", "schedule", "omb no"
- "department", "plan number", "file as"
- "identity of", "maturity date", "collateral"

**Preservation**:
- Participant loan entries (for QA validation)

#### 6.5 Remove Duplicates
```python
remove_duplicates(rows, verbose=True)
```
**Deduplication Key**: `(pdf_stem, issuer_name, investment_description, current_value)`

#### 6.6 Orchestration
```python
clean_investment_data(rows, preserve_loans=True, remove_dupes=True, verbose=True)
```
Returns: `(clean_rows, removed_total_rows)`

---

### Stage 7: Asset Type Enhancement

**Module**: `enhance_asset_types.py`, `fix_asset_types.py`

**Purpose**: Populate missing asset_type fields using pattern matching.

**Asset Type Patterns**:
```python
ASSET_PATTERNS = {
    'MUFXX': 'Money Market Fund',
    'MNY MKT': 'Money Market Fund',
    'Index Fund': 'Index Fund',
    'Mutual Fund': 'Mutual Fund',
    'Common Stock': 'Common Stock',
    'ETF': 'Exchange-Traded Fund',
    'Collective Trust': 'Collective Trust Fund',
    'Separately Managed': 'Separately Managed Account',
    ...
}
```

**Process**:
1. Query investments with missing asset_type
2. Search description for known patterns
3. Update database records
4. Log enhancement statistics

---

### Stage 8: Database Persistence

**Module**: `src/load_db.py`

**Purpose**: Store structured data in SQLite database.

---

### Stage 9: Plan Details Extraction

**Module**: `extract_plan_details.py`

**Purpose**: Extract plan name (Part II - 1a) and sponsor name (Part II - 1b) from Form 5500 PDFs and update the database.

**Process**:
1. **Extract from Schedule H pages** (most reliable):
   - Search last 20 pages for Schedule H
   - Extract plan name from header (e.g., "Amazon 401(k) Plan")
   - Extract sponsor from EIN line or plan name prefix
   - Clean up trailing text like "EIN #..." or "Plan #..."

2. **Fallback to filename inference**:
   - Pattern matching on company names in filenames
   - Apply default sponsor/plan name mappings
   - Handle common company patterns (Amazon, Apple, Google, Microsoft, etc.)

3. **Update database**:
   - Update `plans` table with plan_name and sponsor fields
   - Maintain data consistency across all related investments
   - Export updated data to clean CSV

**Key Functions**:
- `extract_part_ii_fields()` - Extract from PDF
- `update_plan_details()` - Update database with extracted data
- `export_updated_csv()` - Refresh CSV exports

**Output**:
- Updated `plans` table with plan_name and sponsor columns
- Refreshed `investments_clean.csv` with sponsor and plan_name

**Example Results**:
```
EIN: 91-1986545
Plan Number: 001
Sponsor: Amazon.com, Inc.
Plan Name: Amazon 401(k) Plan
```

---

### Stage 10: Investment Data Cleanup & Standardization

**Module**: `cleanup_investment_names.py`

**Purpose**: Separate issuer names from investment descriptions and standardize naming conventions.

**Process**:
1. **Parse Combined Fields**:
   - Identify when issuer_name contains both issuer and fund name
   - Extract asset manager from fund names (e.g., "VANGUARD TARGET 2025" → Issuer: "Vanguard", Fund: "Target Retirement 2025 Fund")

2. **Standardize Issuer Names**:
   - Vanguard funds: Remove "VANGUARD"/"VANG" prefix, consolidate to "Vanguard"
   - BlackRock funds: Standardize LifePath funds to "BlackRock"
   - PIMCO funds: Clean abbreviations like "TOTAL RTN" → "Total Return Fund"
   - American Funds: Convert "AF" prefix to "American Funds"
   - Other firms: BNY Mellon, State Street, Baillie Gifford, etc.

3. **Clean Investment Descriptions**:
   - Expand abbreviations: "INST" → "Institutional", "IDX" → "Index"
   - Expand truncations: "INTL" → "International", "STK" → "Stock", "MKT" → "Market"
   - Format target date funds: "TARGET 2025" → "Target Retirement 2025 Fund"
   - Remove asset type info from descriptions (goes in asset_type field)

4. **Handle Special Cases**:
   - Individual stocks: Issuer and description are the same (company name)
   - Brokerage accounts: Identify self-directed accounts properly
   - Company stock funds: Extract company name correctly

**Key Functions**:
- `parse_issuer_and_investment()` - Intelligent parsing with 15+ patterns
- `cleanup_investments()` - Batch processing with progress reporting
- `export_cleaned_csv()` - Update CSV with cleaned data

**Pattern Examples**:
```python
"VANGUARD TARGET 2025" → 
  Issuer: "Vanguard"
  Description: "Target Retirement 2025 Fund"

"PIMCO TOTAL RTN II" →
  Issuer: "PIMCO"
  Description: "Total Return Fund II"

"VG IS TL INTL STK MK" →
  Issuer: "Vanguard"
  Description: "Total International Stock Market Index Fund"

"BROKERAGE LINK ACCOUNT" →
  Issuer: "Fidelity"
  Description: "BrokerageLink Self-Directed Account"
```

**Restore Missing Data**:

**Module**: `restore_missing_descriptions.py`

- Recovers investment descriptions lost during earlier processing
- Cross-references with raw CSV data
- Ensures 100% data completeness

---

### Stage 11: LLM-Enhanced Data Quality

**Module**: `llm_enhance_investments.py`

**Purpose**: Use OpenAI GPT to review and improve investment data quality beyond rule-based logic.

**Process**:
1. **Batch Processing**:
   - Process investments in configurable batches (default: 10 records)
   - Send to GPT-4o-mini for intelligent review
   - Rate limiting to respect API constraints

2. **LLM Review Tasks**:
   - **Issuer Name**: Ensure only asset manager/firm name (e.g., "Vanguard", not "Vanguard Target 2025")
   - **Investment Description**: Extract and format fund names properly
   - **Asset Type**: Standardize to consistent categories
   - **Abbreviation Expansion**: Handle non-standard abbreviations
   - **Context-Aware Decisions**: Use financial knowledge for edge cases

3. **Quality Improvements**:
   - Catch cases missed by rule-based logic
   - Handle variations in fund naming conventions  
   - Standardize inconsistent abbreviations
   - Preserve individual stock names correctly

**Configuration**:
```bash
# Basic usage (process all records)
python llm_enhance_investments.py

# Custom batch size
python llm_enhance_investments.py --batch-size 15

# Test on first 2 batches only
python llm_enhance_investments.py --max-batches 2

# Custom paths
python llm_enhance_investments.py --db data/outputs/pipeline.db --output data/outputs/investments_clean.csv
```

**Key Functions**:
- `review_investment_with_llm()` - Send batch to GPT for review
- `llm_enhance_investments()` - Orchestrate batch processing
- `export_enhanced_csv()` - Export improved data

**Example Improvements**:
```
Before: "VG IS TL INTL STK MK"
After:  Issuer: "Vanguard"
        Description: "Total International Stock Market Index Fund Institutional Shares"

Before: "HARRIS OAKMRK INTL 3"
After:  Issuer: "Harris Associates"
        Description: "Oakmark International Fund Class 3"

Before: "GALLIARD SHORT CORE FUND F (Fair value)"
After:  Issuer: "Galliard"
        Description: "Short Core Fund F"
```

**Results**:
- Enhanced 133 of 216 records (61.6%)
- 100% data completeness across all fields
- Consolidated issuers from 153 → 149 (better standardization)
- Improved readability and consistency

**Reports**:
- `show_cleanup_summary.py` - Basic statistics
- `show_llm_improvements.py` - Detailed improvement analysis
- `show_final_summary.py` - Complete cleanup summary

---

### Stage 12: Final Database Update

**Module**: `src/load_db.py`

**Schema** (SQL: `sql/schema.sql`):

**Tables**:
1. **plans** - Pension plan metadata
   - sponsor_ein, plan_number, plan_year (COMPOSITE PRIMARY KEY)
   - **plan_name** - Plan name extracted from Form 5500 Part II-1a (e.g., "Amazon 401(k) Plan")
   - **sponsor** - Sponsor name extracted from Schedule H or EIN line (e.g., "Amazon.com, Inc.")
   - administrator_name, administrator_address
   - plan_type
   - plan_year_begin, plan_year_end
   - total_participants_bol, total_participants_eol
   - total_assets_bol, total_assets_eol
   - total_liabilities_bol, total_liabilities_eol
   - source_pdf, updated_at

2. **investments** - Investment holdings
   - sponsor_ein, plan_number, plan_year (COMPOSITE FOREIGN KEY → plans)
   - page_number, row_id
   - issuer_name, investment_description
   - asset_type
   - par_value, cost, current_value, units_or_shares
   - confidence

3. **source_pages** - Page metadata
   - sponsor_ein, plan_number, plan_year (COMPOSITE FOREIGN KEY → plans)
   - page_number, is_supplemental
   - image_path

4. **ocr_cells** - Raw OCR data
   - sponsor_ein, plan_number, plan_year (COMPOSITE FOREIGN KEY → plans)
   - page_number, row_id, cell_id
   - bbox, text, confidence

**Functions**:
- `init_db()` - Create schema from SQL file
- `load_to_db()` - Bulk insert operations

---

## 📊 Data Schema

### Investment Record Schema

**Configuration**: `config/schema.yml`

```yaml
schema:
  fields:
    - issuer_name              # issuer or obligor name
    - investment_description   # description or type of investment
    - asset_type              # category (mutual fund, stock, etc.)
    - par_value               # principal amount or face value
    - cost                    # book value or cost basis
    - current_value           # fair value or market value (REQUIRED)
    - units_or_shares         # number of shares or units held
    - page_number             # source page in PDF
    - row_id                  # row index within page

  required:
    - issuer_name
    - current_value

  numeric_fields:
    - par_value
    - cost
    - current_value
    - units_or_shares

  header_synonyms:
    issuer_name:
      - "issuer"
      - "name of issuer"
      - "issuer or obligor"
      - "identity of issue"
    
    investment_description:
      - "description"
      - "investment description"
      - "type of investment"
      - "title of issue"
    
    current_value:
      - "current value"
      - "fair value"
      - "market value"
      - "value"
    
    # ... (see config/schema.yml for complete list)
```

### Database Data Types

| Field | SQLite Type | Notes |
|-------|-------------|-------|
| issuer_name | TEXT | Full issuer name |
| investment_description | TEXT | Investment details |
| asset_type | TEXT | Asset category |
| par_value | TEXT | Stored as text for precision |
| cost | TEXT | Stored as text for precision |
| current_value | TEXT | Stored as text for precision |
| units_or_shares | TEXT | Stored as text for precision |
| page_number | INTEGER | Source page reference |
| row_id | INTEGER | Row index reference |
| confidence | REAL | OCR confidence score (0-1) |

---

## ⚙️ Configuration

### Environment Variables (`.env`)

```bash

# Data Paths
INPUT_DIR_RAW=data/inputs/raw
INPUT_DIR_PRO=data/inputs/processed
OUTPUT_DIR=data/outputs

# EC2 / S3 orchestration pattern
# Keep the pipeline pointed at local EC2 directories, then sync with S3 outside the app:
# S3 raw input:      s3://retirementinsights-bronze/filings_5500_pdf/year=2024/raw/
# S3 processed input:s3://retirementinsights-bronze/filings_5500_pdf/year=2024/processed/
# S3 output:         s3://retirementinsights-silver/tables/

# Processing Options
DPI=350                    # Image resolution for OCR
USE_OCR=0                  # 0=text extraction, 1=OCR mode
USE_LLM=1                  # Enable GPT-based column mapping

# OpenAI Configuration
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-5.2

# Configuration Files
KEYWORDS_YML=config/keywords.yml
SCHEMA_YML=config/schema.yml
SCHEMA_SQL=sql/schema.sql
```

### Keywords Configuration (`config/keywords.yml`)

```yaml
# Page Classification Keywords
supplemental_schedule_keywords:
  - ASSETS
  - INVESTMENTS
  - SCHEDULE H
  - LINE 4(i)
  - LINE 4i
  - CURRENT VALUE

negative_keywords:
  - TRANSACTIONS
  - LOANS OUTSTANDING
  - SCHEDULE G

# Classification Thresholds
min_keyword_hits: 1
header_scan_max_lines: 12
```

### Validation Configuration (`config/schema.yml`)

```yaml
validation:
  confidence_threshold: 0.75        # Standard OCR confidence
  low_confidence_threshold: 0.6     # Minimum acceptable confidence
  max_currency_len: 32              # Max characters for currency values
  allow_negative: true              # Allow negative values
```

---

## 🚀 Usage

### Complete Pipeline

Run the full extraction and cleaning pipeline:

```bash
python -m src.run_pipeline
```

**What it does**:
1. Reads PDFs from `INPUT_DIR_RAW` (default: `data/inputs/raw/`)
2. Saves raw CSV: `data/outputs/investments_raw.csv`
3. Applies comprehensive cleaning
4. Saves clean CSV: `data/outputs/investments_clean.csv`
5. Saves removed rows: `data/outputs/removed_total_rows.csv`
6. Updates SQLite database: `data/outputs/pipeline.db`
7. Enhances missing asset types
8. Moves processed PDFs to `INPUT_DIR_PRO` (default: `data/inputs/processed/`)
9. Prints summary statistics

**Important**:
- `INPUT_DIR_RAW`, `INPUT_DIR_PRO`, and `OUTPUT_DIR` must be local filesystem paths.
- If you run on EC2 with S3-backed inputs/outputs, sync files from S3 down to the local EC2 folders before running the pipeline, then sync results back to S3 after the run.

### Individual Scripts

#### 1. Run Core Pipeline
```bash
# Set paths explicitly if needed
export INPUT_DIR_RAW=data/inputs/raw
export INPUT_DIR_PRO=data/inputs/processed
export OUTPUT_DIR=data/outputs

# With OCR
export USE_OCR=1
python -m src.run_pipeline

# Text-only (faster)
export USE_OCR=0
python -m src.run_pipeline
```

#### 2. Clean Existing CSV
```bash
python cleanup_with_dedup.py
```
Applies cleaning to existing `investments_clean.csv`.

#### 3. Update Database from Clean CSV
```bash
python update_db_with_clean_csv.py
```
Reloads database from cleaned CSV file.

#### 4. Enhance Asset Types
```bash
python enhance_asset_types.py
```
Populates missing asset_type fields.

#### 5. QA Check
```bash
python qa_check.py
```
Validates data quality and prints report.

### Debug Utilities

```bash
# Inspect specific pages
python debug_pages_85_86.py

# Debug table detection
python debug_tables.py

# Debug header detection
python debug_header_detection.py

# Debug column mapping
python debug_columns.py

# Check Amazon 401k extraction
python check_amazon_investment_pages.py
python inspect_amazon_pages.py
python debug_amazon_extraction.py
```

---

## 📤 Output Files

### 1. investments_raw.csv
**Location**: `data/outputs/investments_raw.csv`

**Description**: Unprocessed extraction results directly from OCR/table extraction.

**Contains**:
- All detected investment rows
- Includes total rows
- Includes metadata rows
- May contain duplicates
- Includes split rows

**Use Case**: Debugging, audit trail, comparing before/after cleaning

---

### 2. investments_clean.csv
**Location**: `data/outputs/investments_clean.csv`

**Description**: **Production-ready cleaned and enhanced investment data**.

**Cleaning Applied**:
- ✅ Total rows removed
- ✅ Metadata rows removed
- ✅ Duplicates removed
- ✅ Split rows merged
- ✅ Asset types extracted
- ✅ Issuer names standardized
- ✅ Investment descriptions cleaned
- ✅ LLM-enhanced for quality
- ✅ Plan names and sponsors added
- ✅ Data validated (100% complete)

**Columns**:
```
sponsor_ein, plan_number, plan_year, sponsor, plan_name,
issuer_name, investment_description, asset_type, par_value, cost, 
current_value, units_or_shares, page_number, row_id, confidence
```

**Data Quality Metrics**:
- 216 investment records
- 100% complete (all fields populated)
- 149 unique issuers (consolidated and standardized)
- 9 asset type categories
- 133 records enhanced by LLM (61.6%)

**Use Case**: Final dataset for analysis, reporting, downstream systems

---

### 3. removed_total_rows.csv
**Location**: `data/outputs/removed_total_rows.csv`

**Description**: Rows identified and removed as totals/summaries.

**Purpose**: Manual verification and audit of cleaning logic.

**Review**: Check this file to ensure legitimate investments weren't mistakenly removed.

---

### 4. pipeline.db
**Location**: `data/outputs/pipeline.db`

**Description**: SQLite relational database with full data model.

**Tables**: plans, investments, source_pages, ocr_cells

**Query Examples**:
```sql
-- Total investments by plan
SELECT p.plan_name, p.sponsor, COUNT(*) as count, SUM(i.current_value) as total_value
FROM investments i
JOIN plans p ON i.sponsor_ein = p.sponsor_ein 
    AND i.plan_number = p.plan_number 
    AND i.plan_year = p.plan_year
GROUP BY p.sponsor_ein, p.plan_number, p.plan_year;

-- Top 10 holdings across all plans
SELECT issuer_name, investment_description, current_value, 
       sponsor_ein, plan_number, plan_year
FROM investments
ORDER BY CAST(current_value AS REAL) DESC
LIMIT 10;

-- Investments by asset type
SELECT asset_type, COUNT(*) as count, SUM(CAST(current_value AS REAL)) as total_value
FROM investments
WHERE asset_type IS NOT NULL AND asset_type != ''
GROUP BY asset_type
ORDER BY total_value DESC;

-- Plans with most diverse holdings
SELECT p.sponsor_ein, p.plan_number, p.plan_year, p.plan_name, 
       COUNT(DISTINCT i.asset_type) as asset_types,
       COUNT(*) as total_holdings
FROM plans p
JOIN investments i ON p.sponsor_ein = i.sponsor_ein 
    AND p.plan_number = i.plan_number 
    AND p.plan_year = i.plan_year
GROUP BY p.sponsor_ein, p.plan_number, p.plan_year
ORDER BY asset_types DESC
LIMIT 20;
```

---

### 5. qa_report.json
**Location**: `data/outputs/qa_report.json`

**Description**: JSON validation report with quality metrics.

**Contents**:
```json
{
  "total_pages": 15,
  "total_rows": 1247,
  "required_field_compliance": 0.98,
  "avg_confidence": 0.89,
  "low_confidence_rows": 23,
  "validation_errors": [...]
}
```

---

## ☁️ AWS Deployment

The pipeline supports deployment to AWS Batch for large-scale processing.

### Architecture
- **ECR**: Docker container registry
- **AWS Batch**: Compute environment (Fargate)
- **S3**: Input/output storage

### Setup Scripts

#### 1. Deploy Docker Image to ECR
```bash
./deploy_to_ecr.sh
```
Builds and pushes Docker image to AWS ECR.

#### 2. Create AWS Batch Environment
```bash
./setup_batch.sh          # EC2-based compute
./setup_batch_fargate.sh  # Fargate serverless
```

#### 3. Monitor Jobs
```bash
./monitor_job.sh <job-id>
```

### Configuration
See `AWS_BATCH_GUIDE.md` for detailed AWS deployment instructions.

### Dockerfile
```dockerfile
FROM python:3.14-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "complete_pipeline.py"]
```

---

## 🧪 Testing

### Test Scripts

```bash
# Test data cleaner
python test_cleanup.py

# Test total removal logic
python test_total_removal.py
```

### Manual Inspection

```bash
# Analyze total rows
python analyze_totals.py

# Check cleanup results
python cleanup_totals.py
```

---

## 📝 Development Notes

### Key Design Decisions

1. **Text vs OCR Mode**:
   - Text mode (default): Faster, works for well-formatted PDFs
   - OCR mode: More robust, handles scanned/image-based PDFs

2. **Preservation of Participant Loans**:
   - Kept even without values for QA validation
   - Helps auditors verify completeness

3. **Asset Type Extraction**:
   - Pattern-based (fast, deterministic)
   - Could be enhanced with ML classification

4. **Total Row Detection**:
   - Rule-based with fund name preservation
   - Regularly updated patterns based on real-world cases

5. **Database Storage**:
   - Numeric fields stored as TEXT for precision
   - Converted to REAL only for calculations
   - Avoids floating-point precision issues

### Performance Considerations

- **Text mode**: 10-15 seconds per PDF
- **OCR mode**: 2-3 minutes per PDF
- **Database inserts**: Batched for efficiency
- **LLM calls**: Cached column mappings where possible

### Extensibility

To add new asset types:
1. Update patterns in `enhance_asset_types.py`
2. Add synonyms to `config/schema.yml`
3. Update documentation

To support new form variations:
1. Add keywords to `config/keywords.yml`
2. Update header matching in `src/llm_map.py`
3. Test with sample documents

---

## 🤝 Contributing

### Code Style
- Follow PEP 8
- Use type hints where applicable
- Document complex functions
- Add logging for debugging

### Testing New PDFs
1. Place PDF in `data/inputs/raw/`
2. Run `python -m src.run_pipeline`
3. Check `removed_total_rows.csv` for false positives
4. Verify investment_clean.csv accuracy
5. Update patterns if needed

---

## 📚 References

### Form 5500 Documentation
- [DOL Form 5500](https://www.dol.gov/agencies/ebsa/employers-and-advisers/plan-administration-and-compliance/reporting-and-filing/form-5500)
- [Schedule H Instructions](https://www.dol.gov/sites/dolgov/files/EBSA/employers-and-advisers/plan-administration-and-compliance/reporting-and-filing/form-5500/schedule-h-instructions.pdf)

### Library Documentation
- [pdfplumber](https://github.com/jsvine/pdfplumber)
- [Camelot](https://camelot-py.readthedocs.io/)
- [PaddleOCR](https://github.com/PaddlePaddle/PaddleOCR)
- [OpenAI API](https://platform.openai.com/docs)

---

## 📄 License

[Specify License]

---

## 📧 Contact

For questions or issues, please contact [Your Contact Information].

---

**Last Updated**: February 15, 2026  
**Version**: 2.0  
**Pipeline Status**: Production Ready with LLM Enhancement ✅

---

## 🆕 Recent Improvements (v2.0)

### Plan Details Extraction
- ✅ Automatic extraction of plan names from Form 5500 Part II
- ✅ Sponsor name identification from Schedule H pages
- ✅ Fallback logic for filename-based inference
- ✅ Database and CSV updates with plan metadata

### Investment Data Standardization
- ✅ Intelligent separation of issuer names from fund descriptions
- ✅ Standardization of asset manager names (Vanguard, BlackRock, etc.)
- ✅ Abbreviation expansion (INST→Institutional, IDX→Index, etc.)
- ✅ Proper formatting of fund names and share classes
- ✅ Recovery of missing descriptions from raw data

### LLM-Enhanced Quality Control
- ✅ OpenAI GPT-4o-mini integration for intelligent data review
- ✅ Context-aware improvements beyond rule-based logic
- ✅ Batch processing with rate limiting
- ✅ Enhanced 133/216 records (61.6%) with AI-powered improvements
- ✅ Comprehensive reporting and validation

### Data Quality Achievements
- ✅ 100% field completeness (all 216 records have all required fields)
- ✅ 149 unique issuers (consolidated from 153)
- ✅ Standardized asset type categories
- ✅ Production-ready data with audit trails
