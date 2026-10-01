
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

SPARK_SCRIPT = "/opt/airflow/scripts/q2/silver_spark.py"

with DAG(
    dag_id="silver_realestate_transform",
    schedule="@monthly",
    start_date=datetime(2026, 7, 1),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["realestate", "silver"],
) as dag:

    # 같은 logical date 의 bronze DAG run 이 success 될 때까지 대기
    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "spark-submit --master 'local[*]' --name silver_realestate_transform "
            "--packages com.databricks:spark-xml_2.12:0.18.0 "
            f"{SPARK_SCRIPT} "
            "--bucket $S3_BUCKET "
            "--yyyymm {{ data_interval_start.strftime('%Y%m') }} "

        ),
    )

    wait_bronze >> spark_transform
