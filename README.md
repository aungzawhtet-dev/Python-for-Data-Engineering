# Python-for-Data-Engineering
This repository contains an 8-week journey into Python, Pandas, Databases, ETL pipelines, and Apache Airflow. This learning plan is designed to move from foundations → applied projects → production-style workflows, with mini-projects each week and capstone projects at the end.

Overview :

Week 1 – Python Foundations Learn the essentials of Python for data tasks: lists, dictionaries, sets, functions, exception handling, and file I/O. Mini-project: merge multiple CSVs, clean duplicates, and save to a new file.

Week 2 – Pandas Basics Work with Pandas for ETL-like tasks: DataFrames, indexing, filtering, groupby, joins, and handling missing values. Mini-project: clean a sales dataset and calculate total revenue by region.

Week 3 – Python + Databases Connect Python to SQLite/Postgres, create tables, insert rows, query data, and integrate logging/config handling. Mini-project: extract data from an API, transform with Pandas, and load into a database.

Week 4 – Pure Python Pipelines Write simple ETL scripts in Python: extract, transform, and load functions chained together with logging and error handling. Mini-project: build a full ETL pipeline with data quality checks.

Week 5 – Airflow Basics Get started with Apache Airflow: install via Docker, explore the UI, and create DAGs using PythonOperator and BashOperator. Mini-project: DAG that reads a CSV, cleans it with Pandas, and saves the output.

Week 6 – Airflow + Databases Use Airflow to move data into databases: configure connections, use PostgresOperator, pass data with XComs, and practice backfill/catchup. Mini-project: end-to-end DAG that transforms a CSV and loads it into Postgres.

Week 7 – Advanced DAGs Build more complex DAGs with branching, retries, multi-dataset workflows, and data quality checks (e.g., row counts). Mini-project: compare row counts between MongoDB and Postgres.

Week 8 – Capstone Projects 

Apply everything in larger projects: ETL DAG from MongoDB → Pandas → Postgres using Airflow. The pipeline extracts documents from MongoDB, transforms them with Pandas (cleaning ObjectIds, normalizing columns, serializing nested structures), and loads them into a Postgres table with enforced schema. The DAG demonstrates modular ETL tasks (extract, transform, load) with XCom for data passing. Scheduling can be configured for batch vs micro-batch pipelines (daily vs every minute), enabling comparison of execution time, error handling, and throughput.
