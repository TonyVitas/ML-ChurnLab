# Credit Card Churn Model - Pipeline Diagrams

---

## 1. High-Level Architecture

```mermaid
graph TB
    subgraph External["External Systems"]
        ORA[(Oracle DB (Configured via ORACLE_DSN))]
    end

    subgraph Config["Configuration"]
        ENV["Environment Variables: ORACLE_USER, PASSWORD, DSN"]
        CFG["src/config.py: DB, SQL_PARAMS, OUTPUT_CSV, STAGING_DIR, WINSOR percentiles"]
    end

    subgraph Orchestration["Orchestration Layer"]
        PREFECT["Prefect 3 Flow: churn_etl_pipeline"]
        TASKS["Prefect Tasks: extract, validate, transform, save"]
        ARTIFACTS["Prefect Artifacts: Markdown validation reports"]
    end

    subgraph Core["Core Pipeline"]
        DBM["OracleConnectionManager - src/db_manager.py"]
        ETL["Feature Engineering - src/etl/pipeline/etl_steps.py"]
        VAL["DataValidator - src/etl/pipeline/validator.py"]
        SCH["Pandera Schemas - src/etl/pipeline/schemas.py"]
    end

    subgraph Outputs["Outputs"]
        STG["staging/run_id - parquet snapshots"]
        CSV["output - base_features.csv"]
        LOGS["logs - pipeline_*.log"]
    end

    ENV --> CFG
    CFG --> PREFECT
    PREFECT --> TASKS
    TASKS --> DBM
    DBM --> ORA
    TASKS --> ETL
    TASKS --> VAL
    VAL --> SCH
    VAL --> ARTIFACTS
    TASKS --> STG
    TASKS --> CSV
    PREFECT --> LOGS
```

---

## 2. End-to-End ETL Pipeline Flow (Prefect)

```mermaid
flowchart TD
    START([python run.py]) --> INIT["Setup file logger, Generate run_id"]

    INIT --> E1

    subgraph STEP1["Step 1 — Extract"]
        E1["extract_from_db - retries=3, delay=30s"]
        E1 --> V1["validate_step - RawExtractSchema"]
        V1 --> S1["save_staging_parquet - 01_raw_extract.parquet"]
    end

    S1 --> T2

    subgraph STEP2["Step 2 — Lagged Originals"]
        T2["task_create_lagged_originals - 20 lag columns added"]
        T2 --> V2["validate_step - LaggedOriginalsSchema"]
    end

    V2 --> T3

    subgraph STEP3["Step 3 — Engineered Base"]
        T3["task_create_engineered_base - 9+ engineered columns"]
        T3 --> V3["validate_step - EngineeredBaseSchema"]
    end

    V3 --> T4

    subgraph STEP4["Step 4 — Lagged and Derived Features"]
        T4["task_create_engineered_lagged_and_derived - lags, deltas, pct_chg, trends, volatility"]
        T4 --> V4["validate_step - FinalFeaturesSchema"]
    end

    V4 --> T5

    subgraph STEP5["Step 5 — Winsorisation"]
        T5["task_winsorise_features - clip at 1st/99th percentile"]
        T5 --> WA["create Winsorisation Prefect artifact"]
    end

    WA --> T6

    subgraph STEP6["Step 6 — Churn Targets"]
        T6["task_create_churn_targets - CHURN_1M, 2M, 3M"]
        T6 --> V6["validate_step - FeaturesTargetSchema"]
    end

    V6 --> SAVE

    subgraph STEP7["Step 7 — Persist"]
        SAVE["save_staging_parquet - 06_final_features.parquet"]
        SAVE --> CSVOUT["save_output_csv - output/base_features.csv"]
    end

    CSVOUT --> DONE([Pipeline Complete])

    style STEP1 fill:#e8eef4,stroke:#7a8a9a
    style STEP2 fill:#e8f0e8,stroke:#7a8a7a
    style STEP3 fill:#e8f0e8,stroke:#7a8a7a
    style STEP4 fill:#e8f0e8,stroke:#7a8a7a
    style STEP5 fill:#f0ebe4,stroke:#8a7a6a
    style STEP6 fill:#f0e8ec,stroke:#8a7a7a
    style STEP7 fill:#ebe8f0,stroke:#7a7a8a
```

---

## 3. Database Extraction Flow

```mermaid
sequenceDiagram
    participant Flow as Prefect Flow
    participant Task as extract_from_db
    participant Mgr as OracleConnectionManager
    participant Pool as oracledb Pool
    participant DB as Oracle DB

    Flow->>Task: extract_from_db(sql_path)
    Task->>Mgr: __init__(user, password, dsn)
    Mgr->>Pool: create_pool(min=1, max=5)

    Task->>Mgr: get_connection()
    Mgr->>Pool: acquire()
    Pool->>DB: connect
    DB-->>Pool: connection
    Mgr->>DB: SELECT 1 FROM DUAL
    DB-->>Mgr: OK (validated)
    Mgr-->>Task: connection

    Task->>Task: Read SQL file
    Task->>DB: cursor.execute(query)
    DB-->>Task: rows + column descriptions
    Task->>Task: pd.DataFrame(rows, columns)

    Task->>Mgr: close()
    Mgr->>Pool: close(force=True)

    Task-->>Flow: raw_df

    Note over Task: On failure: retries up to 3x with 30s delay between attempts
```

---

## 4. OracleConnectionManager Internals

```mermaid
stateDiagram-v2
    [*] --> NoPool: __init__()

    NoPool --> PoolActive: get_connection() / connection()
    note right of PoolActive: Lazy pool creation, min=1 max=5

    state PoolActive {
        [*] --> Acquire: pool.acquire()
        Acquire --> Validate: SELECT 1 FROM DUAL
        Validate --> Valid: success
        Validate --> Invalid: failure

        Invalid --> RetryAcquire: close and re-acquire
        RetryAcquire --> ReValidate
        ReValidate --> Valid: success
        ReValidate --> Error: second failure

        Valid --> [*]: return connection
        Error --> [*]: raise Error
    }

    PoolActive --> Closed: close()
    Closed --> [*]
```

---

## 5. Feature Engineering Pipeline — Detailed

```mermaid
flowchart LR
    subgraph INPUT["Raw Extract"]
        RAW["raw_df - 1 row per CARD_ID per MONTH_END"]
    end

    subgraph S1["Step 1: create_lagged_originals"]
        direction TB
        SORT1["Sort by CARD_ID, MONTH_END"]
        LAG["group_by CARD_ID, shift(n) for n=1,2,3,6"]
        COLS1["+20 columns: TOTAL_EXPOSURE_AMT, CARD_LIMIT, DPD, OVERDUE_AMT_RON, UNUTILIZED_AMT at 1,2,3,6M"]
        SORT1 --> LAG --> COLS1
    end

    subgraph S2["Step 2: create_engineered_base"]
        direction TB
        META["MONTH_YYYYMM - category"]
        ACTIVE["IS_ACTIVE_MTH - STATUS, DPD, last_exp"]
        DEMO["AGE, TENURE_MONTHS, ADVERTISING_CONSENT"]
        UTIL["UTILIZATION_NOW - exposure / limit"]
        STMT["PCT_INSTALLMENT_REMAINING_MTH, STMT_INSTLMNT_REMAIN_AMT_MTH"]
        ISSUE["HAS_ISSUE_MTH"]
        META ~~~ ACTIVE ~~~ DEMO ~~~ UTIL ~~~ STMT ~~~ ISSUE
    end

    subgraph S3["Step 3: create_engineered_lagged_and_derived"]
        direction TB
        ELAGS["A) Engineered Lags: UTILIZATION_NOW, TRX_AMT_MTH, TRX_CNT_MTH, AVG_TICKET_MTH at 1,2,3,6M"]
        DELTAS["B) Deltas: 8 metrics x 4 horizons - current minus lagged"]
        PCTCHG["C) Pct Changes: 5 metrics x 4 horizons - (current-lag)/lag x 100"]
        TRENDS["D) Trends: EXPOSURE_TRENDING_UP, DPD_TRENDING_UP, UTILIZATION_TRENDING_UP"]
        VOLAT["E) Volatility: EXPOSURE_RANGE_MTH, DPD_RANGE_MTH"]
        ELAGS ~~~ DELTAS ~~~ PCTCHG ~~~ TRENDS ~~~ VOLAT
    end

    subgraph S4["Step 4: winsorise_features"]
        direction TB
        W["Clip numerics at 1st / 99th percentile"]
        WEXCL["Exclude: IDs, flags, categoricals, targets"]
        WREP["winsor_report artifact - col to n_clipped"]
        W ~~~ WEXCL ~~~ WREP
    end

    subgraph S5["Step 5: create_churn_targets"]
        direction TB
        CANCEL["CARD_CANCELLATION_DATE vs MONTH_END"]
        HORIZONS["months_to_cancel = year x 12 + month diff"]
        FLAGS["CHURN_1M: less than 1 month, CHURN_2M: less than 2 months, CHURN_3M: less than 3 months"]
        CANCEL --> HORIZONS --> FLAGS
    end

    RAW --> S1 --> S2 --> S3 --> S4 --> S5

    style INPUT fill:#e8eef4,stroke:#7a8a9a
    style S1 fill:#e8f0e8,stroke:#7a8a7a
    style S2 fill:#e8f0e8,stroke:#7a8a7a
    style S3 fill:#e8f0e8,stroke:#7a8a7a
    style S4 fill:#f0ebe4,stroke:#8a7a6a
    style S5 fill:#f0e8ec,stroke:#8a7a7a
```

---

## 6. Data Validation Flow

```mermaid
flowchart TD
    IN["DataFrame + Schema + step_name"]

    IN --> STATS["_stats_markdown - shape, null percent per column"]
    IN --> LOG["_log_stats - log to file logger"]

    STATS --> VALIDATE["schema.validate(df, lazy=True) - Pandera lazy validation"]
    LOG --> VALIDATE

    VALIDATE -->|Success| PASS
    VALIDATE -->|SchemaErrors| FAIL

    subgraph PASS["Validation PASSED"]
        PASS_DF["Return validated DataFrame - dtypes coerced"]
        PASS_MD["Markdown Report: shape, nulls, PASSED (SchemaName)"]
    end

    subgraph FAIL["Validation FAILED"]
        FAIL_LOG["Log each failure: column, check, failure_case"]
        FAIL_DF["Return original DataFrame - pipeline continues"]
        FAIL_MD["Markdown Report: shape, nulls, error table max 30 rows"]
        FAIL_LOG ~~~ FAIL_DF ~~~ FAIL_MD
    end

    PASS --> ART["create_markdown_artifact - Prefect UI artifact"]
    FAIL --> ART

    style PASS fill:#d8e6d8,stroke:#6a7a6a
    style FAIL fill:#e6d8d8,stroke:#7a6a6a
```

---

## 7. Schema Progression (Column Accumulation)

```mermaid
graph LR
    subgraph RAW["RawExtractSchema"]
        R1["Identifiers: CARD_ID, MONTH_END"]
        R2["Card Snapshot: 30+ cols - CARD_LIMIT, DPD, STATUS..."]
        R3["Customer: 8 cols - CNTRY, EDUCATION, BIRTH_DT..."]
        R4["Statement: 12 cols - FEE_AMOUNT, TRX_AMT_RON..."]
        R5["Profit: 7 cols - TOTAL_PROFIT, FEE..."]
        R6["Transaction: 6 cols - TRX_CNT_MTH, AVG_TICKET..."]
        R7["TLMK: NUMBER_OF_ISSUES"]
        R8["DF checks: no duplicate rows, UNUTILIZED <= LIMIT x 1.05, DPD monotonicity"]
    end

    subgraph LAG["LaggedOriginalsSchema"]
        L1["= RawExtract +"]
        L2["20 lag columns - 5 metrics x 4 horizons"]
    end

    subgraph ENG["EngineeredBaseSchema"]
        E1["= LaggedOriginals +"]
        E2["9 engineered base cols - MONTH_YYYYMM, IS_ACTIVE_MTH, AGE, UTILIZATION_NOW..."]
    end

    subgraph FIN["FinalFeaturesSchema"]
        F1["= EngineeredBase +"]
        F2["16 eng. lags, 32 deltas, 20 pct changes, 3 trends, 2 volatility"]
    end

    subgraph TAR["FeaturesTargetSchema"]
        T1["= FinalFeatures +"]
        T2["CHURN_1M, CHURN_2M, CHURN_3M"]
    end

    RAW ==>|"+20 cols"| LAG ==>|"+9 cols"| ENG ==>|"+73 cols"| FIN ==>|"+3 cols"| TAR

    style RAW fill:#e8eef4,stroke:#7a8a9a
    style LAG fill:#e8f0e8,stroke:#7a8a7a
    style ENG fill:#e8f0e8,stroke:#7a8a7a
    style FIN fill:#f0ebe4,stroke:#8a7a6a
    style TAR fill:#f0e8ec,stroke:#8a7a7a
```

---

## 8. Winsorisation Process

```mermaid
flowchart TD
    IN["Input DataFrame"]

    IN --> SELECT["Select numeric columns - df.select_dtypes number"]
    SELECT --> FILTER["Filter out excluded columns - IDs, flags, categoricals, targets"]

    FILTER --> LOOP["For each candidate column"]

    LOOP --> QUANT["Compute quantiles: lo = quantile(0.01), hi = quantile(0.99)"]
    QUANT --> CHECK{lo == hi?}

    CHECK -->|Yes| SKIP["Skip column - constant distribution"]
    CHECK -->|No| COUNT["Count outliers: values below lo or above hi"]

    COUNT --> HAS{"n > 0?"}
    HAS -->|No| SKIP
    HAS -->|Yes| CLIP["col.clip(lower=lo, upper=hi), report col = n"]

    CLIP --> LOOP
    SKIP --> LOOP

    LOOP -->|all done| REPORT["Store report in df.attrs"]
    REPORT --> LOGR["Log: N columns, total values clipped, Top 15 by count DEBUG"]
    LOGR --> OUT["Return clipped DataFrame"]

    style IN fill:#e8eef4
    style OUT fill:#d8e6d8,stroke:#6a7a6a
```

---

## 9. Churn Target Creation Logic

```mermaid
flowchart TD
    IN["Input DataFrame"]
    IN --> PARSE["Parse dates: month_end = pd.to_datetime MONTH_END, cancel_dt = pd.to_datetime CARD_CANCELLATION_DATE"]

    PARSE --> CALC["months_to_cancel = (cancel.year minus end.year) x 12 + (cancel.month minus end.month)"]

    CALC --> FUTURE{"cancel_dt not null AND months_to_cancel > 0?"}

    FUTURE -->|No| ZERO["CHURN_1M = 0, CHURN_2M = 0, CHURN_3M = 0"]
    FUTURE -->|Yes| HORIZON

    subgraph HORIZON["Horizon Check"]
        H1{"months_to_cancel <= 1?"}
        H2{"months_to_cancel <= 2?"}
        H3{"months_to_cancel <= 3?"}
    end

    H1 -->|Yes| C1["CHURN_1M = 1"]
    H1 -->|No| C1N["CHURN_1M = 0"]
    H2 -->|Yes| C2["CHURN_2M = 1"]
    H2 -->|No| C2N["CHURN_2M = 0"]
    H3 -->|Yes| C3["CHURN_3M = 1"]
    H3 -->|No| C3N["CHURN_3M = 0"]

    C1 & C1N & C2 & C2N & C3 & C3N --> OUT["Output DataFrame with targets"]

    style IN fill:#e8eef4
    style HORIZON fill:#f0e8ec,stroke:#8a7a7a
    style OUT fill:#d8e6d8
```

---

## 10. Data I/O and Persistence Flow

```mermaid
flowchart LR
    subgraph Sources["Data Sources"]
        ORA[(Oracle DB)]
        SQL["SQL files: sample_extract.sql, base_extract.sql"]
    end

    subgraph Pipeline["Pipeline Processing"]
        DF["pd.DataFrame - in-memory"]
    end

    subgraph Staging["Staging Layer"]
        S1["staging run_id - 01_raw_extract.parquet"]
        S6["staging run_id - 06_final_features.parquet"]
    end

    subgraph Output["Final Output"]
        CSV["output - base_features.csv"]
    end

    subgraph Logs["Logging"]
        FLOG["logs pipeline_ts.log - DEBUG-level file log"]
        PLOG["Prefect UI - task-level logs"]
        PART["Prefect Artifacts - validation and winsor reports"]
    end

    SQL --> ORA
    ORA -->|"cursor.execute to fetchall"| DF
    DF -->|"Step 1: to_parquet"| S1
    DF -->|"Step 7: to_parquet"| S6
    DF -->|"Step 7: to_csv"| CSV
    DF -.->|"each step"| FLOG
    DF -.->|"each task"| PLOG
    DF -.->|"validation steps"| PART

    style Sources fill:#e8eef4,stroke:#7a8a9a
    style Pipeline fill:#ebe8e4,stroke:#8a7a6a
    style Staging fill:#ebe8f0,stroke:#7a7a8a
    style Output fill:#d8e6d8,stroke:#6a7a6a
    style Logs fill:#e0e0e0,stroke:#7a7a7a
```

---

## 11. Prefect Task Dependency Graph

```mermaid
graph TD
    EXT["extract_from_db - tags: extract, oracle, retries: 3 x 30s"]
    VAL1["validate_step - RawExtractSchema"]
    SAV1["save_staging_parquet - 01_raw_extract"]

    LAG["task_create_lagged_originals"]
    VAL2["validate_step - LaggedOriginalsSchema"]

    ENG["task_create_engineered_base"]
    VAL3["validate_step - EngineeredBaseSchema"]

    DER["task_create_engineered_lagged_and_derived"]
    VAL4["validate_step - FinalFeaturesSchema"]

    WIN["task_winsorise_features"]

    CHR["task_create_churn_targets"]
    VAL5["validate_step - FeaturesTargetSchema"]

    SAV2["save_staging_parquet - 06_final_features"]
    CSVT["save_output_csv"]

    EXT --> VAL1
    VAL1 --> SAV1
    VAL1 --> LAG
    LAG --> VAL2
    VAL2 --> ENG
    ENG --> VAL3
    VAL3 --> DER
    DER --> VAL4
    VAL4 --> WIN
    WIN --> CHR
    CHR --> VAL5
    VAL5 --> SAV2
    VAL5 --> CSVT

    classDef extract fill:#e8eef4,stroke:#7a8a9a
    classDef transform fill:#e8f0e8,stroke:#7a8a7a
    classDef validate fill:#ebe8e4,stroke:#8a7a6a
    classDef io fill:#ebe8f0,stroke:#7a7a8a

    class EXT extract
    class LAG,ENG,DER,WIN,CHR transform
    class VAL1,VAL2,VAL3,VAL4,VAL5 validate
    class SAV1,SAV2,CSVT io
```

---

## 12. Collective End-to-End Diagram

```mermaid
flowchart TB
    subgraph ENV["Environment and Config"]
        direction LR
        EVAR["ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN"]
        CONF["config.py - DB, SQL_PARAMS, WINSOR percentiles"]
        EVAR --> CONF
    end

    subgraph ORCH["Prefect 3 Orchestration"]
        direction TB
        FLOW["@flow churn_etl_pipeline - log_prints=True, on_failure=_on_flow_failure"]
        FLOG["File Logger - logs pipeline_*.log"]
        RUNID["run_id - flow_run.get_id or timestamp"]
        FLOW --> FLOG
        FLOW --> RUNID
    end

    subgraph EXTRACT["1 — Extract"]
        direction TB
        DBMGR["OracleConnectionManager - pool min=1 max=5"]
        SQLF["sample_extract.sql"]
        XQUERY["cursor.execute to fetchall"]
        RAWDF["raw_df"]
        DBMGR --> XQUERY
        SQLF --> XQUERY
        XQUERY --> RAWDF
    end

    subgraph VALID_RAW["Validate: RawExtractSchema"]
        direction TB
        VR_STATS["Stats: shape, nulls"]
        VR_CHECK["Checks: no dupes, UNUTILIZED <= LIMIT x 1.05, DPD monotonicity"]
        VR_ART["Prefect Artifact"]
        VR_STATS --> VR_CHECK --> VR_ART
    end

    subgraph TRANSFORM["2–6 — Feature Engineering"]
        direction TB
        T1["create_lagged_originals - +20 lag columns"]
        TV1["Validate: LaggedOriginalsSchema"]
        T2["create_engineered_base - +9 engineered columns"]
        TV2["Validate: EngineeredBaseSchema"]
        T3["create_engineered_lagged_and_derived - +73 derived columns"]
        TV3["Validate: FinalFeaturesSchema"]
        T4["winsorise_features - clip 1st to 99th percentile"]
        T5["create_churn_targets - CHURN_1M, 2M, 3M"]
        TV4["Validate: FeaturesTargetSchema"]

        T1 --> TV1 --> T2 --> TV2 --> T3 --> TV3 --> T4 --> T5 --> TV4
    end

    subgraph PERSIST["7 — Persist"]
        direction LR
        PQ1["staging run_id - 01_raw_extract.parquet"]
        PQ2["staging run_id - 06_final_features.parquet"]
        OCSV["output - base_features.csv"]
    end

    ENV --> ORCH
    ORCH --> EXTRACT
    EXTRACT --> VALID_RAW
    VALID_RAW --> PQ1
    VALID_RAW --> TRANSFORM
    TRANSFORM --> PQ2
    TRANSFORM --> OCSV

    style ENV fill:#f0f0f0,stroke:#8a8a8a
    style ORCH fill:#e8eef4,stroke:#7a8a9a
    style EXTRACT fill:#e8eef4,stroke:#7a8a9a
    style VALID_RAW fill:#ebe8e4,stroke:#8a7a6a
    style TRANSFORM fill:#e8f0e8,stroke:#7a8a7a
    style PERSIST fill:#ebe8f0,stroke:#7a7a8a
```

---

## 13. Error Handling and Resilience

```mermaid
flowchart TD
    subgraph Retry["DB Extract Retry Strategy"]
        A1["extract_from_db Attempt 1"] -->|fail| W1["Wait 30s"]
        W1 --> A2["Attempt 2"] -->|fail| W2["Wait 30s"]
        W2 --> A3["Attempt 3"] -->|fail| FAIL_EXTRACT["Task FAILED"]
        A1 -->|success| OK["Continue pipeline"]
        A2 -->|success| OK
        A3 -->|success| OK
    end

    subgraph Validation["Validation Error Handling"]
        VIN["validate(df, schema)"]
        VIN -->|SchemaErrors| WARN["Log warnings, Return original df, pipeline continues"]
        VIN -->|Success| PASS["Return validated df, dtypes coerced"]
    end

    subgraph FlowFail["Flow-Level Failure"]
        FFAIL["Any unhandled exception"]
        FFAIL --> HOOK["_on_flow_failure - print error to console"]
        HOOK --> FUTURE["Extend: Slack or email - placeholder"]
    end

    subgraph ConnPool["Connection Pool Resilience"]
        ACQ["pool.acquire()"]
        ACQ --> VAL{"validate_connection - SELECT 1 FROM DUAL"}
        VAL -->|OK| USE["Use connection"]
        VAL -->|Fail| RETRY["Close and re-acquire"]
        RETRY --> VAL2{"validate again?"}
        VAL2 -->|OK| USE
        VAL2 -->|Fail| ERR["Raise oracledb.Error"]
    end

    style Retry fill:#f0ebe4,stroke:#8a7a6a
    style Validation fill:#ebe8e4,stroke:#8a7a6a
    style FlowFail fill:#f0e8ec,stroke:#8a7a7a
    style ConnPool fill:#e8eef4,stroke:#7a8a9a
```
