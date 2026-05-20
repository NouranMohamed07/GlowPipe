from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'glowpipe',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='glowpipe_daily_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 5, 19),
    catchup=False,
    tags=['batch', 'glowpipe'],
) as dag:

    bronze_task = BashOperator(
        task_id='bronze_ingestion',
        bash_command="""
            docker exec spark_master \
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /opt/airflow/batch/spark_jobs/bronze.py
        """,
    )

    silver_task = BashOperator(
        task_id='silver_cleaning',
        bash_command="""
            docker exec spark_master \
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /opt/airflow/batch/spark_jobs/silver.py
        """,
    )

    gold_task = BashOperator(
        task_id='gold_feature_build',
        bash_command="""
            docker exec spark_master \
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /opt/airflow/batch/spark_jobs/gold.py
        """,
    )

    def snowflake_placeholder():
        print("Snowflake load - not implemented yet")

    snowflake_task = PythonOperator(
        task_id='snowflake_load',
        python_callable=snowflake_placeholder,
    )

    bronze_task >> silver_task >> gold_task >> snowflake_task