# HDB Resale Flat Prices ETL Pipeline

**Senior Data Engineer Technical Test**

A production-ready ETL pipeline for processing HDB resale flat price data (2012-2016) from data.gov.sg, implementing clean architecture and best practices.

---

## 📐 Design Philosophy

This project follows **separation of concerns** and **configuration-driven** principles:

1. **No Business Logic in DAG**: Airflow DAG contains only task definitions with `op_kwargs`
2. **Function-Based Operations**: Pure functions in `data_operations/` for all logic
3. **Configuration-Driven Validation**: DQ checks defined in config, DAG loops to create tasks
4. **File-Based Data Passing**: Clear staging pattern without database dependency

---

## 📁 Project Structure

```
hdb_technical_test/
├── config/
│   ├── api_config.py           # API endpoints, resource IDs, file paths
│   └── validation_rules.py     # DQ checks configuration
│
├── data_operations/
│   ├── extract.py              # fetch_and_save_from_api(), merge_raw_files(), extract_and_merge()
│   ├── validate.py             # check_null(), check_categorical(), check_outliers(), etc.
│   └── transform.py            # calculate_remaining_lease(), create_resale_identifier(), hash_identifiers()
│
├── dags/
│   └── hdb_etl_pipeline.py     # Clean DAG - loops over DQ_CHECKS
│
├── notebooks/
│   └── exploration.ipynb       # Exploratory data analysis
│
├── data/
│   ├── raw/                    # Individual API files + merged_raw.csv
│   ├── stage/                  # validated.csv + profile.json
│   ├── prod/                   # transformed.csv + hashed.csv
│   └── failed/                 # failed_records.csv
│
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🔄 Data Flow & File-Based Passing

### Why File-Based?
Since this assignment doesn't require a database, we use a **clear staging pattern** with CSV files:

```
┌─────────────────────────────────────────────────────────────┐
│  EXTRACT & MERGE                                            │
│  - Fetch from API → data/raw/raw_hdb_*.csv                  │
│  - Merge all → data/raw/merged_raw.csv                      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  PROFILE & VALIDATE                                         │
│  - Generate profile → data/stage/profile.json               │
│  - Run DQ checks (looped tasks)                             │
│  - Check duplicates & outliers                              │
│  - Separate → data/stage/validated.csv                      │
│              data/failed/failed_records.csv                 │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                  │
│  - Read: data/stage/validated.csv                           │
│  - Transform → data/prod/transformed.csv (plain IDs)        │
│               data/prod/hashed.csv (hashed IDs)             │
└─────────────────────────────────────────────────────────────┘
```

### Data Passing Strategy

| Stage | Reads From | Writes To | Why |
|-------|------------|-----------|-----|
| **Extract** | API | `raw/raw_hdb_*.csv`, `raw/merged_raw.csv` | Preserve individual API responses + merged dataset |
| **Validate** | `raw/merged_raw.csv` | `stage/validated.csv`, `failed/failed_records.csv` | Separate valid/failed records |
| **Transform** | `stage/validated.csv` | `prod/transformed.csv`, `prod/hashed.csv` | Final outputs |

**Benefits:**
- ✅ Easy to inspect data at any stage
- ✅ Rerun individual stages independently
- ✅ No database setup required
- ✅ Production-ready pattern (works with S3/HDFS)

---

## 🎯 Key Design Decisions

### 1. Simplified extract.py

**Three functions only:**
```python
fetch_and_save_from_api(resource_id)  # Fetch one resource, save to CSV
merge_raw_files(raw_dir)               # Merge all raw CSVs
extract_and_merge(raw_dir)             # Main function: calls both above
```

**Why?**
- Clear, straightforward flow
- `extract_and_merge()` is what DAG calls
- No complex class hierarchies

### 2. Simplified transform.py

**Three transformations only:**
```python
calculate_remaining_lease(df)      # Transformation 1
create_resale_identifier(df)       # Transformation 2 (all digit extraction embedded)
hash_identifiers(df)                # Transformation 3
```

**Why?**
- Each function = one transformation
- All helper logic (extract_block_digits, etc.) embedded in `create_resale_identifier()`
- No separate utility functions cluttering the module

### 3. Individual Check Functions in validate.py

**Each check is a separate function:**
```python
check_null(df, column, allow_null)
check_categorical(df, column, allowed_values)
check_date_range(df, column, min_date, max_date)
check_duplicates(df, key_columns, strategy)
check_outliers(df, column, method, threshold, group_by)
separate_valid_failed(input_file, validation_results)
```

**Why?**
- DAG can call individual checks as tasks
- Easy to add new check types
- Each check returns boolean Series (True = valid)

### 4. Configuration-Driven DQ Checks

**In `config/validation_rules.py`:**
```python
DQ_CHECKS = {
    "null": {
        "resale_price": {"allow_null": False},
        "town": {"allow_null": False},
    },
    "categorical": {
        "town": {"allowed_values": []},  # Populated from profile
        "flat_type": {"allowed_values": []},
    },
    "date_range": {
        "month": {"min_date": None, "max_date": None},
    },
}
```

**DAG loops over this:**
```python
dq_tasks = []
for check_type, check_config in DQ_CHECKS.items():
    for column, params in check_config.items():
        dq_task = PythonOperator(
            task_id=f"dq_check_{check_type}_{column}",
            python_callable=validate_data,
            op_kwargs={
                "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
                "check_name": check_type,
                "column": column,
                **params
            },
            dag=dag,
        )
        dq_tasks.append(dq_task)
```

**Benefits:**
- ✅ Add new checks by editing config only
- ✅ DAG automatically creates tasks
- ✅ Clean separation of what (config) vs how (code)

### 5. Separate Valid/Failed as Own Task

Instead of mixing validation with separation, we have:
1. Individual DQ check tasks (return boolean Series)
2. **Separate task**: `separate_valid_failed()`
   - Combines all validation results
   - Splits into `validated.csv` + `failed_records.csv`

**Why?**
- Clear responsibility separation
- Failed records tracked with reasons
- Easier to debug which checks failed

---

## 📊 ETL Pipeline Tasks

### Task Flow in DAG

```
extract_and_merge
       ↓
  generate_profile
       ↓
   ┌───┴────────────────────────────┐
   │                                │
DQ Checks (looped)          Duplicate Check
   │                                │
   │                         Outlier Check
   │                                │
   └───┬────────────────────────────┘
       ↓
separate_valid_failed
       ↓
  transform_data
```

### Task Breakdown

| Task | Function | Input | Output |
|------|----------|-------|--------|
| `extract_and_merge` | `extract_and_merge()` | API | `raw/merged_raw.csv` |
| `generate_profile` | `generate_profile()` | `raw/merged_raw.csv` | `stage/profile.json` |
| `dq_check_null_*` (looped) | `validate_data()` | `raw/merged_raw.csv` | Boolean Series |
| `dq_check_categorical_*` (looped) | `validate_data()` | `raw/merged_raw.csv` | Boolean Series |
| `dq_check_date_range_*` (looped) | `validate_data()` | `raw/merged_raw.csv` | Boolean Series |
| `check_duplicates` | `validate_data()` | `raw/merged_raw.csv` | Boolean Series |
| `check_outliers` | `validate_data()` | `raw/merged_raw.csv` | Boolean Series |
| `separate_valid_failed` | `separate_valid_failed()` | Combined results | `stage/validated.csv` + `failed/failed_records.csv` |
| `transform_data` | `transform_data()` | `stage/validated.csv` | `prod/transformed.csv` + `prod/hashed.csv` |

---

## 🔐 Configuration Management

### API Configuration (`config/api_config.py`)

```python
API_BASE_URL = "https://data.gov.sg/api/action/datastore_search"
API_KEY = os.getenv("DATA_GOV_SG_API_KEY", None)  # From .env

RESOURCE_IDS = [
    "1b702208-44bf-4829-b620-4615ee19b57c",  # 2012-2014
    "83b2fc37-ce8c-4df4-968b-370fd818138b",  # 2015-2016
]

RAW_DATA_DIR = "data/raw"
STAGE_DATA_DIR = "data/stage"
PROD_DATA_DIR = "data/prod"
FAILED_DATA_DIR = "data/failed"
```

### Validation Rules (`config/validation_rules.py`)

- **DQ_CHECKS**: Loopable checks (null, categorical, date_range)
- **DUPLICATE_CHECK**: Separate check config
- **OUTLIER_CHECK**: Separate check config
- **populate_rules_from_profile()**: Dynamically populate from data profile

---

## 🏗️ Best Practices Implemented

### Clean DAG Pattern
- ✅ **Zero function definitions**: Only `PythonOperator` tasks
- ✅ **Loop for DQ checks**: Auto-generate tasks from config
- ✅ **op_kwargs pattern**: All parameters passed declaratively

### File-Based Data Passing
- ✅ **Clear stages**: raw → stage → prod → failed
- ✅ **Inspectable**: Easy to check intermediate results
- ✅ **Rerunnable**: Re-run any stage independently

### Modular Functions
- ✅ **Single responsibility**: Each function does one thing
- ✅ **Pure functions**: No side effects, testable
- ✅ **Embedded logic**: Helper functions inside main functions

### Configuration-Driven
- ✅ **DQ checks in config**: Add checks without code changes
- ✅ **Dynamic rules**: Populated from data profile
- ✅ **Environment variables**: API keys in `.env`

### Data Engineering
- ✅ **Composite key deduplication**: Keep higher price
- ✅ **Multi-method outlier detection**: IQR + Z-score
- ✅ **Audit trail**: Failed records with reasons
- ✅ **Data lineage**: Clear trail from raw to prod

---

## 🚀 Running the Pipeline

### Option 1: Jupyter Notebook (Exploration)

```bash
jupyter notebook notebooks/exploration.ipynb
# Explore data, test transformations
```

### Option 2: Direct Execution (Development)

```python
from data_operations.extract import extract_and_merge
from data_operations.validate import generate_profile, separate_valid_failed
from data_operations.transform import transform_data

# Run pipeline
extract_and_merge(raw_dir="data/raw")
generate_profile(input_file="data/raw/merged_raw.csv", output_file="data/stage/profile.json")
# ... run individual checks ...
separate_valid_failed(input_file="data/raw/merged_raw.csv", validation_results=...)
transform_data(input_file="data/stage/validated.csv")
```

### Option 3: Airflow (Production)

```bash
# Initialize
airflow db init

# Copy DAG
cp dags/hdb_etl_pipeline.py ~/airflow/dags/

# Start services
airflow webserver &
airflow scheduler &

# Trigger
airflow dags trigger hdb_resale_etl
```

---

## 📦 Installation

```bash
# Clone repository
git clone <repo-url>
cd hdb_technical_test

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment (optional)
cp .env.example .env
# Edit .env if you have API keys
```

---

## 📝 Assignment Requirements Coverage

| Requirement | Implementation | File |
|-------------|----------------|------|
| Programmatic extraction | ✅ API-based, no manual downloads | `extract.py` |
| Combine datasets | ✅ `merge_raw_files()` | `extract.py` |
| Data profiling | ✅ `generate_profile()` | `validate.py` |
| Validation rules | ✅ Dynamic from profile | `validate.py`, `validation_rules.py` |
| Duplicate handling | ✅ Composite key, keep higher price | `validate.py::check_duplicates()` |
| Remaining lease | ✅ 99-year, rounded down | `transform.py::calculate_remaining_lease()` |
| Resale Identifier | ✅ Multi-step logic embedded | `transform.py::create_resale_identifier()` |
| SHA-256 hashing | ✅ Irreversible with uniqueness check | `transform.py::hash_identifiers()` |
| Anomaly detection | ✅ IQR + Z-score, grouped | `validate.py::check_outliers()` |
| Output datasets | ✅ raw, stage, prod, failed | All modules |
| Jupyter notebook | ✅ Exploration-focused | `notebooks/exploration.ipynb` |
| Airflow DAG | ✅ Clean, looped DQ checks | `dags/hdb_etl_pipeline.py` |
| Best practices | ✅ Modular, documented, maintainable | All files |

---

**Built with clean architecture for HDB Data Engineering Team**
