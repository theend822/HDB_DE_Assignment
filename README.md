# HDB Resale Prices ETL Pipeline

---

## Design Philosophy
- The DAG only wires tasks together — no logic, no conditionals, no function definitions
- All business logic lives in `data_operations/`, independently testable and reusable
- Config drives what checks run and on which columns — adding a check means editing config, not code
- Every stage writes to CSV so intermediate results are always inspectable

---

## Project Structure

```
HDB/
├── config/
│   ├── CONFIG_hdb_resales_price.py   # API config, dataset IDs, folder paths
│   └── DQC_hdb_resales_price.py      # DQ check rules (DQ_CHECKS, DUPLICATE_CHECK, RESALE_PRICE_OUTLIER_CHECK)
│
├── data_operations/
│   ├── extract.py                    # fetch_and_save_from_api(), merge_raw_files()
│   ├── validate.py                   # check_*() functions, separate_valid_failed()
│   └── transform.py                  # calculate_remaining_lease(), create_resale_identifier(), hash_identifiers()
│
├── dags/
│   └── hdb_resales_price.py          # Airflow DAG — task definitions only, no business logic
│
├── notebooks/
│   └── execution.ipynb              # Prototype before migrating to data_operations/
│
└── data/
    ├── raw/                          # Per-dataset CSVs + merged_raw.csv
    ├── stage/                        # validated.csv + dqc_results/ (one file per check)
    ├── prod/                         # transformed.csv + hashed.csv
    └── failed/                       # non_valid_records.csv
```

---

## Overall Design

### a. Data Flow

Each stage reads from CSV and writes to CSV. No database required.

```
API
 ↓
data/raw/{name}.csv          ← one file per dataset (5 total)
 ↓
data/raw/merged_raw.csv      ← all datasets merged
 ↓
data/stage/dqc_results/      ← one 0/1 result file per DQC check (parallel)
 ↓
data/stage/validated.csv     ← rows that passed all checks
data/failed/non_valid_records.csv
 ↓
data/prod/transformed.csv    ← with remaining_lease + resale_identifier
data/prod/hashed.csv         ← with resale_identifier_hash instead
```


### b. DAG

The DAG has four logical blocks

```python
# Block 1: Extract
download_tasks = [...]   # 5 parallel download tasks, one per dataset
merge_task               # waits for all downloads

# Block 2: DQC
dqc_tasks = [...]        # auto-generated from DQ_CHECKS config
check_duplicates_task
check_outlier_task

# Block 3: Separate and transform
separate_task
transform_task

# Dependencies
download_tasks >> merge_task >> all_dqc >> separate_task >> transform_task
```

---

## Design Considerations

### a. Modular Design

The pipeline is broken into independent modules, each with a single responsibility:

| Module | Location | Responsibility |
|--------|----------|----------------|
| **Pipeline** | `dags/hdb_resales_price.py` | Task wiring only — no business logic |
| **Functions** | `data_operations/` | Reusable extract, validate, transform functions |
| **DQC config** | `config/DQC_hdb_resales_price.py` | DQ check rules and thresholds |
| **Pipeline config** | `config/CONFIG_hdb_resales_price.py` | API config, dataset IDs, folder paths |

This separation means:
- Functions in `data_operations/` are independently testable and reusable across pipelines
- Adding or modifying a DQ check means editing config, not touching the DAG or functions
- The DAG stays clean — it only wires tasks together using `PythonOperator` + `op_kwargs`

All files belonging to one pipeline share the same `_{pipeline_name}` suffix (`hdb_resales_price`). Adding a new pipeline means adding one DAG, one CONFIG, one DQC — all with a matching suffix. This makes it immediately clear which files belong together and keeps maintenance straightforward.

### b. Config-Driven Pipeline

DQ checks are defined entirely in `DQC_hdb_resales_price.py`. The DAG loops over `DQ_CHECKS` and auto-generates one Airflow task per `(check_type, column)` pair — no manual task creation needed when a new check is added.

The two standalone checks (duplicates, outlier) follow the same pattern: their parameters come from `DUPLICATE_CHECK` and `RESALE_PRICE_OUTLIER_CHECK` and are passed directly as `op_kwargs` via `**unpacking`. The DAG never needs to know what the parameters mean.

### c. `fail_sum` Across Parallel DQC Tasks

Each DQC check runs as a separate Airflow task in parallel on the same static `merged_raw.csv`. Writing to a shared file concurrently would cause race conditions.

The solution: each check writes its own 0/1 result column (1 = row failed) to a uniquely named file in `stage/dqc_results/`. The `separate_valid_failed` task then reads all result files and sums them row-by-row — any row with `fail_sum > 0` failed at least one check and goes to `non_valid_records.csv`.

This gives a clean audit trail: each result file shows exactly which rows failed which check.

## Future Improvements
### a.Dataset Partition

In the context of this assignment, dataset is saved and passed around in csv. In real world, for better query performance, merged dataset should be properly partitioned - by year or by combination of registration / approval date

### b. More Customized DQC

Only one customized DQC is implemented in this assignment to monitor the fluctuation of resale prices in each self-defined group. However, in real world, to keep close eye on HDB resale prices, more customized and realistic DQC should be created and implemented in the pipeline. 