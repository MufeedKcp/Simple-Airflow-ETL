# Airflow-DAG-ETL

A Dockerized Apache Airflow DAG ETL pipeline that downloads a CSV dataset from [CSV]("https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"), loads it into a MySQL staging table, and merges the staged records into a target table on a scheduled DAG run.

This repos is fully dedicated  to learn [Apache Airflow](https://airflow.apache.org/) based on the official Apache Airflow pipeline tutorial. The goal is not to simulate a production system, but to understand how Airflow actually defines, schedules, executes, and tracks ETL workflows.

---
# This is What I Learned from this Project: 

1. How to define DAGs using the TaskFlow API (`@dag` and `@task`).
2. Configuring DAG scheduling with cron expressions.
3. Understanding parameters `dagrun_timeout`, `catch_up`, `schedule`.
4. Creating task dependencies and controlling execution order.
5. How to connect and execute Airflow provider operators (`SQLExecuteQueryOperator`).
6. Connecting Airflow tasks to MySQL through an Airflow connection ID.
7. How to connect MySQL database using `MySqlHook` for database operations that require Python-level control.
8. Loading CSV data into MySQL with `LOAD DATA LOCAL INFILE`.
9. Separating staging and target tables in an ETL workflow.
10. Implementing an upsert/merge pattern with MySQL `ON DUPLICATE KEY UPDATE`.
11. Running Airflow and MySQL in Docker containers.
12. Understanding how Airflow's scheduler, metadata database, workers/executors, and web UI fit together.

---
## Local Setup

Install:
1. [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) 
2. A GitHub clone of this repository

---
##### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/Simple-Airflow-ETL.git
```

---
##### 2. Start Airflow

```bash
docker compose up -d
```

```bash
docker compose ps
```

----
### 3. Create the MySQL Airflow connection

```bash
docker compose exec airflow-cli airflow connections add first_airflow_conn \
  --conn-type mysql \
  --conn-host mysql \
  --conn-login <MYSQL_USER> \
  --conn-password <MYSQL_PASSWORD> \
  --conn-schema <MYSQL_DATABASE> \
  --conn-port 3306
```
