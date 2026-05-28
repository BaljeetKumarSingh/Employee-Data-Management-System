# Employee Data Management System

A cloud-native Data Warehouse (DWH) pipeline built on AWS that ingests, processes, and reports on employee data — including leave tracking, designation reporting, and real-time communication monitoring via Kafka.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [AWS Services Used](#aws-services-used)
- [Data Sources](#data-sources)
- [Data Layer Design](#data-layer-design)
- [Pipeline Jobs](#pipeline-jobs)
  - [Daily Jobs (07:00 UTC)](#daily-jobs-0700-utc)
  - [Yearly Jobs (Jan 1st)](#yearly-jobs-jan-1st)
  - [Monthly Job (1st of month, 07:00 UTC)](#monthly-job-1st-of-month-0700-utc)
  - [Streaming Pipeline (Continuous)](#streaming-pipeline-continuous)
- [Airflow DAGs](#airflow-dags)
- [Database Schema (PostgreSQL)](#database-schema-postgresql)
- [Project Structure](#project-structure)
- [Setup & Deployment](#setup--deployment)

---

## Architecture Overview

```
Data Sources (S3 / Kafka)
        │
        ▼
  Bronze Layer (S3) ──── Raw ingestion, file-tracking logs
        │
        ▼
  Silver Layer (S3) ──── Cleaned, validated data
        │
        ▼
   Gold Layer (S3 + PostgreSQL) ──── Business logic, reporting tables
        │
        ▼
  Airflow (MWAA / EC2) ──── Orchestration & scheduling
```

The system follows a **Bronze → Silver → Gold** medallion architecture. Raw files land in S3 (Bronze), are cleaned and deduplicated (Silver), and final reporting tables are written to both S3 (Gold Parquet) and PostgreSQL on EC2.

The streaming subsystem runs independently on EC2: a Kafka producer emits employee messages, a PySpark Structured Streaming consumer flags reserved words in real time, and two scheduled Spark jobs handle daily salary deduction tracking and monthly strike cooldown resets.

---

## AWS Services Used

| Service | Role |
|---|---|
| **S3** | Primary data lake — Bronze, Silver, and Gold layers |
| **AWS Glue** | Serverless Spark jobs for all batch ETL tasks |
| **Amazon MWAA / Airflow** | DAG orchestration and scheduling |
| **EC2 (Ubuntu)** | Hosts Kafka broker, Kafka producer, PySpark streaming consumer, and PostgreSQL |
| **PostgreSQL (on EC2)** | Operational reporting database for Gold layer tables |

---

## Data Sources

| File | Frequency | Description |
|---|---|---|
| `employee_data.csv` | Daily (S3 drop) | Employee ID, age, name |
| `employee_timeframe_data.csv` / `_*.csv` | Daily incremental (S3 drop) | Designation and salary history per employee; timestamps in Unix epoch |
| `employee_leave_quota_data.csv` | Yearly | Annual leave quota per employee |
| `employee_leave_calendar_data.csv` | Yearly (Jan 1st) | Mandatory company holidays |
| `employee_leave_data.csv` | Daily (07:00 UTC) | Actual leave applications and cancellations |
| `marked_word.json` | Static (S3) | Reserved/flagged words list |
| `vocab.json` | Static (S3) | Full employee message vocabulary |
| Kafka topic `employee-messages` | Real-time stream | JSON messages `{ sender, receiver, message }` |

---

## Data Layer Design

### Bronze (Raw)

Raw files are dropped into S3 prefixes:

```
s3://employee-data-management-system/bronze/employee_data/
s3://employee-data-management-system/bronze/employee_timeframe_data/
s3://employee-data-management-system/bronze/employee_leave_data/
s3://employee-data-management-system/bronze/employee_leave_quota/
s3://employee-data-management-system/bronze/employee_leave_calendar_data/
s3://employee-data-management-system/bronze/json_files/          ← marked_word.json, vocab.json
```

Each Bronze prefix maintains a `processed_files.txt` log (filename → last-modified timestamp) to ensure idempotent, incremental ingestion. Only new or changed files are processed on each run.

### Silver (Cleaned)

Intermediate cleaned data is written to:

```
s3://employee-data-management-system/silver/employee_timeframe_data/
```

### Gold (Business-Ready)

Final Parquet outputs, partitioned where appropriate:

```
s3://employee-data-management-system/gold/employee_data_output/
s3://employee-data-management-system/gold/employee_timeframe_data_output/
s3://employee-data-management-system/gold/employee_leave_data_output/
s3://employee-data-management-system/gold/daily_active_employees_by_designation_output/
s3://employee-data-management-system/gold/employee_80%_parquet_output/
s3://employee-data-management-system/gold/employee_80%_text_output/      ← text files for managers
```

---

## Pipeline Jobs

### Daily Jobs (07:00 UTC)

#### 1. Employee Data (`employee-data-management-system-employee-data`)

- Reads new/updated CSVs from `bronze/employee_data/`.
- Validates columns: `emp_id`, `age`, `name`.
- **Append-only** write to Gold S3 and PostgreSQL table `employee`.

#### 2. Employee Timeframe Data (`employee-data-management-system-employee-timeframe-data`)

- Reads incremental CSVs from `bronze/employee_timeframe_data/`.
- Deduplicates on `(emp_id, start_date, end_date)` — keeps the row with the highest salary.
- Converts Unix epoch timestamps to `DATE`.
- Enforces record continuity: when a new record arrives for an employee, the previous open record's `end_date` is set to the new record's `start_date` and marked `INACTIVE`.
- Records with no `end_date` are marked `ACTIVE`; all others `INACTIVE`.
- Writes to Silver S3 and PostgreSQL table `employee_timeframe`.

#### 3. Employee Leave Data (`employee-data-management-system-employee-leave-data`)

- Reads new/updated CSVs from `bronze/employee_leave_data/`.
- Tracks leave applications and cancellations.
- **Daily append** to Gold S3 and PostgreSQL table `employee_leave`.

#### 4. Count by Designation (`employee-data-management-system-count_by_designation`)

- Reads Gold timeframe data, filters `status = 'ACTIVE'`.
- Groups by `designation`, counts employees.
- Writes snapshot (partitioned by `snapshot_date`) to Gold S3 and **overwrites** PostgreSQL table `employee_designation`.
- Output columns: `designation`, `active_count`, `snapshot_date`.

#### 5. 8% Leave Threshold (`employee-data-management-system-threshold`)

- Calculates upcoming working days from tomorrow through Dec 31 of the current year, excluding weekends and public holidays from the leave calendar.
- Reads employee leave applications; ignores cancelled leaves, duplicate applications, and leaves falling on holidays.
- Flags employees whose pending/upcoming leaves exceed **8% of remaining working days**.
- Output columns: `emp_id`, `upcoming_leaves`.
- Writes to PostgreSQL table `employee_ex`.

---

### Yearly Jobs (Jan 1st)

Both jobs run in **parallel** via the `annual_employee_glue_jobs_parallel` DAG.

#### 6. Leave Quota (`employee-data-management-system-employee-leave-quota`)

- Reads `employee_leave_quota_data.csv` from Bronze.
- Validates columns: `emp_id`, `leave_quota`, `year`.
- **Append-only** write to Gold S3 and PostgreSQL table `leave_quota`.

#### 7. Leave Calendar (`employee-data-management-system-leave-calender-data`)

- Reads `employee_leave_calendar_data.csv` from Bronze.
- Validates columns: `date`, `reason`.
- **Append-only** write to Gold S3 and PostgreSQL table `leave_calendar`.

---

### Monthly Job (1st of month, 07:00 UTC)

#### 8. 80% Quota Report (`employee-data-management-system-Quota_80%`)

- Runs conditionally on the 1st of each month (branched inside the daily DAG).
- Calculates each employee's availed leave as a percentage of their annual quota for the **previous month's** reporting period.
- Employees exceeding **80% quota utilisation** trigger a text file notification (no actual emails).
- Text files are written per manager to `gold/employee_80%_text_output/`.
- A metadata key (`bronze/80%Threshold/metadata.txt`) tracks already-processed periods — if the job fails mid-run and is retried, duplicate reports are not generated.
- Results also written to PostgreSQL table `employee_leaves_exceeding_80`.

---

### Streaming Pipeline (Continuous)

Runs on EC2 with a Python virtual environment (`spark_venv`). Two Airflow DAGs manage the two recurring Spark jobs; the streaming consumer itself runs continuously as a long-running process.

#### Kafka Producer

- Reads `message.json` from disk.
- Publishes each message to Kafka topic `employee-messages` with a 1-second interval.
- Message format:
  ```json
  { "sender": "<emp_id>", "receiver": "<emp_id>", "message": "message body" }
  ```

#### Spark Structured Streaming Consumer

- Subscribes to `employee-messages` topic with `startingOffsets=latest`.
- Loads `marked_word.json` from S3 at startup.
- Parses each message, checks whether the body contains any reserved word.
- Flagged messages are written to PostgreSQL table `flagged_message_history` with a `strike_flag = 1`.
- Uses Kafka message timestamp as the canonical message timestamp.

#### Daily Spark Job — Strike Count (`kafka_dag_daily`, every 5 minutes via `spark-submit every.py`)

- Reads `flagged_message_history` for the past 30 days.
- Aggregates per-employee strike count.
- Joins with active employees from `employee_timeframe`.
- Writes results to PostgreSQL table `strike_count_last_30_days`.

#### Daily Spark Job — Salary Deduction (`kafka_dag_morning`, daily at 00:00 UTC via `spark-submit morning.py`)

- Reads active employees and their base salary from `employee_timeframe`.
- Reads current strike count from `strike_count_last_30_days`.
- Deducts **10% of base salary per strike** — the adjusted salary is stored as a separate column alongside `emp_id` in the output table.
- If an employee reaches **10 strikes**, their status is toggled to `INACTIVE` in `employee_timeframe` and they are excluded from all future cooldown processing.
- **Monthly cooldown** (applied on the 1st): strikes are cleared and salary restored to the pre-strike level (or original salary if only one strike existed). Employees at 10 strikes are excluded from cooldown.

---

## Airflow DAGs

| DAG ID | Schedule | Description |
|---|---|---|
| `daily_employee_glue_jobs_sequential` | `0 7 * * *` | Sequential chain: employee_data → timeframe → leave → designation count → threshold → (conditional) 80% quota |
| `daily_employee_glue_jobs_grouped1` | `0 7 * * *` | Alternative grouped DAG with explicit dependency graph (same jobs, structured differently) |
| `annual_employee_glue_jobs_parallel` | `@yearly` | Parallel run of leave quota and leave calendar Glue jobs on Jan 1st |
| `every_job_dag` | `*/5 * * * *` | Runs `every.py` (strike count aggregation) via `spark-submit` on EC2 |
| `morning_job_dag` | `0 0 * * *` | Runs `morning.py` (salary deduction update) via `spark-submit` on EC2 |

**Dependency graph for the main daily DAG:**

```
employee_data
      │
      ▼
timeframe_data
      │
    ┌─┴──────────────────┐
    ▼                    ▼
leave_data        count_by_designation
    │
    ▼
threshold
    │
    ▼
check_first_day ──► quota_job (1st only)
                └─► skip_quota (all other days)
```

---

## Database Schema (PostgreSQL)

All tables reside on a PostgreSQL instance running on EC2 (`port 5432`, database `postgres`).

| Table | Description |
|---|---|
| `employee` | Append-only employee master (emp_id, name, age) |
| `employee_timeframe` | SCD-style designation/salary history with ACTIVE/INACTIVE status |
| `leave_quota` | Annual leave quota per employee per year |
| `leave_calendar` | Public holiday calendar |
| `employee_leave` | Daily leave application log |
| `employee_designation` | Daily snapshot of active employee count by designation |
| `employee_ex` | Employees exceeding 8% upcoming leave threshold |
| `employee_leaves_exceeding_80` | Employees exceeding 80% quota utilisation (monthly) |
| `flagged_message_history` | Full history of flagged Kafka messages with strike flag |
| `strike_count_last_30_days` | Rolling 30-day strike count per employee |

---

## Project Structure

```
Employee-Data-Management-System-main/
│
├── data/                              # Sample data files
│   ├── employee_data.csv
│   ├── employee_timeframe_data_1.csv
│   ├── employee_leave_quota_data.csv
│   ├── employee_leave_calendar_data.csv
│   ├── marked_word.json
│   ├── vocab.json
│   └── messages.json
│
├── GlueJob.ipynb                      # All 8 AWS Glue job scripts (one cell per task)
│
├── kafka-ec2(4).ipynb                 # Kafka producer + Spark streaming consumer + daily/morning Spark jobs
│
├── 7UTC.py                            # Airflow DAG — sequential daily Glue jobs
├── daily_employee_glue_jobs.py        # Airflow DAG — grouped daily Glue jobs (alternative)
├── yearly.py                          # Airflow DAG — annual Glue jobs (parallel)
├── kafka_dag_daily.py                 # Airflow DAG — every.py (strike count, every 5 min)
└── kafka_dag_morning.py               # Airflow DAG — morning.py (salary update, daily midnight)
```

---

## Setup & Deployment

### Prerequisites

- AWS account with permissions for S3, Glue, MWAA (or self-hosted Airflow), and EC2.
- An EC2 instance (Ubuntu) with:
  - PostgreSQL installed and running on port 5432.
  - Kafka broker running on port 9092.
  - Python virtual environment at `/home/ubuntu/spark_venv/` with PySpark and `kafka-python` installed.
  - JDBC PostgreSQL driver jar available to Spark.

### S3 Setup

1. Create bucket `employee-data-management-system`.
2. Create the Bronze prefixes listed in the [Data Layer Design](#data-layer-design) section.
3. Upload `marked_word.json` and `vocab.json` to `bronze/json_files/`.
4. Place initial data files in their respective Bronze prefixes.

### Glue Jobs

1. Create each Glue job in AWS Glue Studio using the scripts in `GlueJob.ipynb` (one script per task).
2. Name jobs exactly as listed in the [Daily Jobs](#daily-jobs-0700-utc) section — the DAGs reference these names.
3. Attach an IAM role with S3 read/write and Glue execution permissions.
4. Add the PostgreSQL JDBC driver as a Glue dependency.

### Airflow Setup

1. Deploy Airflow (MWAA or self-hosted).
2. Copy all DAG files (`7UTC.py`, `daily_employee_glue_jobs.py`, `yearly.py`, `kafka_dag_daily.py`, `kafka_dag_morning.py`) to the Airflow DAGs folder.
3. Configure the AWS connection (`aws_default`) in Airflow with appropriate credentials.
4. Enable the DAGs in the Airflow UI.

### Kafka & Streaming Setup (EC2)

1. Start the Kafka broker on the EC2 instance.
2. Create the topic:
   ```bash
   kafka-topics.sh --create --topic employee-messages --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
3. Place `every.py` and `morning.py` (from `kafka-ec2(4).ipynb`) at `/home/ubuntu/` on the EC2 instance.
4. Start the Spark Structured Streaming consumer as a background process.
5. Enable `every_job_dag` and `morning_job_dag` in Airflow to drive the periodic Spark jobs.

### PostgreSQL

```sql
-- Verify the connection
psql -h <ec2-public-ip> -U postgres -d postgres
```

Tables are created automatically by the Glue jobs and Spark scripts on first run via JDBC `createTableIfNotExists` or explicit DDL within each job.
