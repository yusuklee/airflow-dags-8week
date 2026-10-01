"""과제 03 — 부동산 실거래가 Gold (집계 + PostgreSQL 적재)

과제 02 DAG(silver_realestate_transform) 완료를 기다린 뒤,
spark-submit 으로 집계 잡(gold_spark_sql.py)을 실행하고 적재 결과를 검증한다.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_SCRIPT = "/opt/airflow/scripts/q3/gold_spark_sql.py"
GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def validate_tables():
    """각 gold 테이블의 row count 가 0 보다 큰지 확인한다."""
    hook = PostgresHook(postgres_conn_id="gold_postgres")
    for table in GOLD_TABLES:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"{table}: {count} rows")
        if count <= 0:
            raise ValueError(f"{table} 테이블이 비어 있습니다")


with DAG(
    dag_id="gold_realestate_aggregate",
    schedule="@monthly",
    start_date=datetime(2026, 7, 1),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["realestate", "gold"],
) as dag:
    # 같은 logical date 의 silver DAG run 이 success 될 때까지 대기
    wait_silver = ExternalTaskSensor(
        task_id="wait_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    spark_aggregate = BashOperator(
        task_id="spark_aggregate",
        bash_command=(
            "spark-submit --master 'local[*]' --name gold_realestate_aggregate "
            f"{SPARK_SCRIPT} --bucket $S3_BUCKET"
        ),
    )

    validate = PythonOperator(task_id="validate_row_count", python_callable=validate_tables)

    wait_silver >> spark_aggregate >> validate
