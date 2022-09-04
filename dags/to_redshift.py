import airflow.utils.dates as dates
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


dag = DAG(
    "create_table",
    schedule_interval=None,
    start_date=dates.days_ago(1),
    template_searchpath="/tmp/"
)

read_from_postgres = PostgresOperator(
    task_id="create_redshift_table",
    postgres_conn_id="redshift",
    sql="create_tables.sql",
    dag=dag,
)