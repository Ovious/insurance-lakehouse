# Insurance Claims Lakehouse

An end-to-end Data Engineering learning project focused on building a modern Lakehouse architecture using **Databricks, PySpark, Delta Lake, dbt, and Python**.

The project simulates data processing for an insurance company, including ingestion, data validation, incremental transformations, data modeling, and orchestration.

It is developed as a hands-on learning project using synthetic insurance data.

## 1. Project Objectives

The main objectives are to:

- Design and implement a Medallion architecture (Bronze, Silver, Gold).
- Develop ETL/ELT pipelines using Python and PySpark.
- Store and process data using Delta Lake.
- Implement incremental data loading and upsert operations.
- Build transformation models using dbt.
- Introduce automated data quality checks and testing.
- Explore workflow orchestration and CI/CD practices.

## 2. Technology Stack

| Technology | Purpose | Status |
|---|---|---|
| Databricks | Data processing platform | In progress |
| PySpark | Data ingestion and transformation | In progress |
| Delta Lake | Storage and transactional tables | In progress |
| Unity Catalog | Data organization and governance | In progress |
| Python | ETL logic and utilities | Planned |
| dbt | SQL transformations and data modeling | Planned |
| GitHub Actions | CI/CD and automated testing | Planned |
| Terraform | Infrastructure as Code exploration | Planned |

## 3. Architecture

The project follows the Medallion architecture:

```text
Synthetic Insurance Data
          |
          v
   BRONZE LAYER
   Raw data ingestion
   Ingestion metadata
   Delta Lake tables
          |
          v
   SILVER LAYER
   Data cleaning
   Validation
   Deduplication
   Incremental MERGE
          |
          v
    GOLD LAYER
    dbt models
    Business aggregations
    Analytical datasets
```

The Bronze layer preserves ingested data, while the Silver and Gold layers will provide progressively refined datasets for analytical use cases.

## 4. Data Model

The project will use synthetic datasets representing an insurance business.

Planned entities:

- **Customers** — customer information and registration details.
- **Policies** — insurance contracts and policy information.
- **Claims** — reported insurance claims and their current status.
- **Claim Events** — historical events associated with insurance claims.

Initial implementation focuses on the `claims` dataset.

## 5. Repository Structure

```text
insurance-lakehouse/
├── notebooks/
│   ├── 01_explore_databricks.py
│   └── 02_bronze_ingestion.py
├── data/
├── dbt/
├── tests/
├── .gitignore
└── README.md
```

Additional directories and modules will be introduced as the project evolves.

## 6. Current Implementation

### Day 1 — Databricks Fundamentals

Initial development includes:

- Databricks workspace exploration.
- Basic Unity Catalog operations.
- Creating PySpark DataFrames with synthetic claims data.
- Adding ingestion metadata.
- Writing data to a Delta Lake Bronze table.
- Identifying invalid records using PySpark filters.

The current dataset includes examples of missing claim identifiers, zero-value claims, and negative claim amounts to demonstrate data quality validation.

## 7. Running the Project

### Requirements

- Databricks Free Edition workspace.
- Access to a Unity Catalog catalog with permission to create schemas and tables.
- Git for version control.

### Execution

1. Open the Databricks workspace.
2. Import or open `01_explore_databricks.py`.
3. Execute the notebook to inspect the environment.
4. Import or open `02_bronze_ingestion.py`.
5. Configure the target catalog and schema.
6. Run the notebook to create the Bronze Delta table.
7. Execute the data quality validation steps.

**Note:** The initial Bronze ingestion uses overwrite mode for educational purposes. Incremental ingestion and idempotent processing will be implemented in later stages.

## 8. Development Roadmap

- [x] Initialize the Databricks learning environment.
- [x] Create the initial synthetic claims dataset.
- [x] Implement the first PySpark ingestion notebook.
- [x] Add basic data quality validation.
- [ ] Implement Delta Lake MERGE and incremental ingestion.
- [ ] Create Silver transformation pipelines.
- [ ] Develop dbt staging and analytical models.
- [ ] Implement dbt tests and documentation.
- [ ] Add Python unit tests.
- [ ] Explore Structured Streaming.
- [ ] Configure Databricks workflows.
- [ ] Implement GitHub Actions CI/CD.
- [ ] Document the final architecture.

## 9. Project Status

**Status: Work in Progress**

This project is being developed incrementally as a practical Data Engineering learning exercise.

All datasets are synthetic. No confidential, proprietary, or production company data is used.