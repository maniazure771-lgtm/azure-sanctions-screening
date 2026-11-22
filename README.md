# Azure Sanctions Screening

An end-to-end sanctions screening data pipeline built using **Azure Data Factory, Azure Databricks, PySpark, ADLS Gen2, and Delta Lake**.

The project demonstrates how customer data can be ingested, validated, standardized, and screened against a sanctions watchlist using a scalable Azure data engineering architecture.

---

## Architecture

```text
                    ┌──────────────────────┐
                    │   Customer /         │
                    │   Watchlist CSV      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      ADLS Gen2       │
                    │       Landing        │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Azure Databricks   │
                    │   Bronze Ingestion   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Silver Transform   │
                    │  Validation &        │
                    │  Standardization     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Gold Matching     │
                    │ Soundex + Levenshtein│
                    │   Match Scoring      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      Gold Delta      │
                    │ Screening Results    │
                    └──────────────────────┘

          Azure Data Factory orchestrates the pipeline
```

---

## Technology Stack

- **Azure Data Factory** – Pipeline orchestration
- **Azure Databricks** – Data processing and matching
- **PySpark** – Transformation and screening logic
- **Azure Data Lake Storage Gen2** – Data storage
- **Delta Lake** – Bronze, Silver and Gold data layers
- **Azure Key Vault** – Secret management
- **GitHub** – Source control
- **Python** – Data processing and automation

---

## Project Structure

```text
azure-sanctions-screening/
│
├── adf/
│   ├── factory/
│   ├── linkedService/
│   ├── pipeline/
│   ├── trigger/
│   └── publish_config.json
│
├── databricks/
│   └── notebooks/
│       ├── 01_bronze_ingestion.py
│       ├── 02_silver_transform.py
│       └── 03_gold_matching.py
│
└── README.md
```

---

## Data Flow

### 1. Landing Layer

Input CSV files are stored in ADLS Gen2:

```text
landing/
└── sanctions_screening/
    ├── customers.csv
    └── sanctions_watchlist.csv
```

The project uses synthetic customer and sanctions watchlist data for demonstration purposes.

---

### 2. Bronze Layer

The Bronze notebook ingests the raw CSV data into Delta format.

```text
bronze/
└── sanctions_screening/
    ├── customers/
    └── watchlist/
```

The ingestion process also captures lineage information such as:

- Source file
- Source system
- Pipeline name
- Ingestion timestamp

---

### 3. Silver Layer

The Silver notebook performs data quality checks and standardization.

Key transformations include:

- Trimming whitespace
- Standardizing customer names
- Converting dates to proper date types
- Standardizing country values
- Removing duplicate customer records
- Validating required fields

Output:

```text
silver/
└── sanctions_screening/
    ├── customers/
    └── watchlist/
```

---

### 4. Gold Layer

The Gold notebook performs sanctions matching between customers and watchlist records.

The matching process includes:

#### Soundex Blocking

Soundex is used to generate a phonetic key for names.

This reduces the number of customer/watchlist combinations that need detailed comparison.

#### Levenshtein Similarity

Levenshtein distance is used to calculate name similarity.

The name similarity contributes up to **60 points** to the overall match score.

#### Date of Birth

An exact date-of-birth match contributes:

```text
25 points
```

#### Country

An exact country match contributes:

```text
15 points
```

### Match Score

```text
Maximum Score = 100

Name      = 60
DOB       = 25
Country   = 15
----------------
Total     = 100
```

### Screening Decision

| Score | Decision |
|---:|---|
| 85+ | MATCH |
| 60–84.99 | REVIEW |
| < 60 | CLEAR |

The final screening results are stored as Delta data in the Gold layer.

```text
gold/
└── sanctions_screening/
    └── screening_results/
```

---

## Azure Data Factory

Azure Data Factory orchestrates the Databricks processing workflow.

Pipeline:

```text
NB_Bronze_Ingestion
        │
        ▼
NB_Silver_Transform
        │
        ▼
NB_Gold_Matching
```

The project also includes a storage event trigger that can start the pipeline when a new CSV file is created in the configured landing location.

---

## Security

Azure Key Vault is used for secure secret management.

The project uses:

- Azure Key Vault
- Managed Identity
- GitHub source control
- Databricks authentication
- ADLS Gen2 access controls

Secrets are not stored directly inside the notebooks.

---

## Sample Data

The current demonstration dataset contains:

- **1,000 customer records**
- **100 sanctions watchlist records**

The data is synthetic and intended only for learning and demonstration.

---

## Current Screening Results

The Gold matching process generated **1,769 candidate pairs** after Soundex-based candidate generation.

Results:

| Decision | Count |
|---|---:|
| CLEAR | 1,563 |
| REVIEW | 181 |
| MATCH | 25 |
| **Total** | **1,769** |

A single customer can have multiple candidate watchlist records, which is why the number of candidate pairs can be greater than the number of customers.

---

## Git Workflow

The project uses a single GitHub repository for both ADF and Databricks:

```text
azure-sanctions-screening
```

ADF source-controlled resources are maintained under:

```text
adf/
```

Databricks notebooks are maintained under:

```text
databricks/notebooks/
```

This keeps the orchestration and data-processing code together as a single project.

---

## Key Learning Areas

This project demonstrates practical experience with:

- Azure Data Factory
- Azure Databricks
- PySpark
- ADLS Gen2
- Delta Lake
- Medallion architecture
- Data quality validation
- Data standardization
- Fuzzy name matching
- Soundex
- Levenshtein distance
- Rule-based screening
- Azure Key Vault
- Managed Identity
- Event-based pipeline triggering
- GitHub source control
- ADF orchestration

---

## Future Enhancements

Potential production enhancements include:

- Incremental processing
- Retry and failure handling
- Pipeline monitoring and alerting
- Audit logging
- Configuration-driven environments
- Dev/Test/Prod deployment
- CI/CD automation
- Additional fuzzy matching techniques
- Token-based name matching
- Customer risk prioritization
- Reporting using Power BI or Azure Synapse

---

# Azure Sanctions Screening

An end-to-end sanctions screening data pipeline built using **Azure Data Factory, Azure Databricks, PySpark, ADLS Gen2, and Delta Lake**.

The project demonstrates how customer data can be ingested, validated, standardized, and screened against a sanctions watchlist using a scalable Azure data engineering architecture.

---

## Architecture

```text
                    ┌──────────────────────┐
                    │   Customer /         │
                    │   Watchlist CSV      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      ADLS Gen2       │
                    │       Landing        │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Azure Databricks   │
                    │   Bronze Ingestion   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Silver Transform   │
                    │  Validation &        │
                    │  Standardization     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Gold Matching     │
                    │ Soundex + Levenshtein│
                    │   Match Scoring      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      Gold Delta      │
                    │ Screening Results    │
                    └──────────────────────┘

          Azure Data Factory orchestrates the pipeline
```

---

## Technology Stack

- **Azure Data Factory** – Pipeline orchestration
- **Azure Databricks** – Data processing and matching
- **PySpark** – Transformation and screening logic
- **Azure Data Lake Storage Gen2** – Data storage
- **Delta Lake** – Bronze, Silver and Gold data layers
- **Azure Key Vault** – Secret management
- **GitHub** – Source control
- **Python** – Data processing and automation

---

## Project Structure

```text
azure-sanctions-screening/
│
├── adf/
│   ├── factory/
│   ├── linkedService/
│   ├── pipeline/
│   ├── trigger/
│   └── publish_config.json
│
├── databricks/
│   └── notebooks/
│       ├── 01_bronze_ingestion.py
│       ├── 02_silver_transform.py
│       └── 03_gold_matching.py
│
└── README.md
```

---

## Data Flow

### 1. Landing Layer

Input CSV files are stored in ADLS Gen2:

```text
landing/
└── sanctions_screening/
    ├── customers.csv
    └── sanctions_watchlist.csv
```

The project uses synthetic customer and sanctions watchlist data for demonstration purposes.

---

### 2. Bronze Layer

The Bronze notebook ingests the raw CSV data into Delta format.

```text
bronze/
└── sanctions_screening/
    ├── customers/
    └── watchlist/
```

The ingestion process also captures lineage information such as:

- Source file
- Source system
- Pipeline name
- Ingestion timestamp

---

### 3. Silver Layer

The Silver notebook performs data quality checks and standardization.

Key transformations include:

- Trimming whitespace
- Standardizing customer names
- Converting dates to proper date types
- Standardizing country values
- Removing duplicate customer records
- Validating required fields

Output:

```text
silver/
└── sanctions_screening/
    ├── customers/
    └── watchlist/
```

---

### 4. Gold Layer

The Gold notebook performs sanctions matching between customers and watchlist records.

The matching process includes:

#### Soundex Blocking

Soundex is used to generate a phonetic key for names.

This reduces the number of customer/watchlist combinations that need detailed comparison.

#### Levenshtein Similarity

Levenshtein distance is used to calculate name similarity.

The name similarity contributes up to **60 points** to the overall match score.

#### Date of Birth

An exact date-of-birth match contributes: 25 points


#### Country

An exact country match contributes:15 points

### Match Score


Maximum Score = 100

Name      = 60
DOB       = 25
Country   = 15
----------------
Total     = 100


### Screening Decision

| Score | Decision |
|---:|---|
| 85+ | MATCH |
| 60–84.99 | REVIEW |
| < 60 | CLEAR |

The final screening results are stored as Delta data in the Gold layer.

```text
gold/
└── sanctions_screening/
    └── screening_results/
```

---

## Azure Data Factory

Azure Data Factory orchestrates the Databricks processing workflow.

Pipeline:

```text
NB_Bronze_Ingestion
        │
        ▼
NB_Silver_Transform
        │
        ▼
NB_Gold_Matching
```

The project also includes a storage event trigger that can start the pipeline when a new CSV file is created in the configured landing location.

---

## Security

Azure Key Vault is used for secure secret management.

The project uses:

- Azure Key Vault
- Managed Identity
- GitHub source control
- Databricks authentication
- ADLS Gen2 access controls

Secrets are not stored directly inside the notebooks.

---

## Sample Data

The current demonstration dataset contains:

- **1,000 customer records**
- **100 sanctions watchlist records**

The data is synthetic and intended only for learning and demonstration.

---

## Current Screening Results

The Gold matching process generated **1,769 candidate pairs** after Soundex-based candidate generation.

Results:

| Decision | Count |
|---|---:|
| CLEAR | 1,563 |
| REVIEW | 181 |
| MATCH | 25 |
| **Total** | **1,769** |

A single customer can have multiple candidate watchlist records, which is why the number of candidate pairs can be greater than the number of customers.

---

## Git Workflow

The project uses a single GitHub repository for both ADF and Databricks:

```text
azure-sanctions-screening
```

ADF source-controlled resources are maintained under:

```text
adf/
```

Databricks notebooks are maintained under:

```text
databricks/notebooks/
```

This keeps the orchestration and data-processing code together as a single project.

---

## Key Learning Areas

This project demonstrates practical experience with:

- Azure Data Factory
- Azure Databricks
- PySpark
- ADLS Gen2
- Delta Lake
- Medallion architecture
- Data quality validation
- Data standardization
- Fuzzy name matching
- Soundex
- Levenshtein distance
- Rule-based screening
- Azure Key Vault
- Managed Identity
- Event-based pipeline triggering
- GitHub source control
- ADF orchestration

---

## Future Enhancements

Potential production enhancements include:

- Incremental processing
- Retry and failure handling
- Pipeline monitoring and alerting
- Audit logging
- Configuration-driven environments
- Dev/Test/Prod deployment
- CI/CD automation
- Additional fuzzy matching techniques
- Token-based name matching
- Customer risk prioritization
- Reporting using Power BI or Azure Synapse

---

## Disclaimer

This project uses synthetic data and is intended for **demonstration purposes only**.

It does not represent a production sanctions screening solution and should not be used for real-world regulatory screening without appropriate controls, validation, security, governance, and compliance review.
