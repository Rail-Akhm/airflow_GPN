from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Rail_Akhm',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(2),
    'retries': 1,

}

with DAG(
    dag_id='dag_with_postgres_opertor_v01',
    default_args=default_args,
    schedule_interval=None
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )