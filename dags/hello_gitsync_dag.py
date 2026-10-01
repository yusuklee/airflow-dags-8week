"""git-sync 동작 확인용 테스트 DAG (BashOperator 만 사용)"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_gitsync_dag",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["gitsync", "test"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'hello from git-sync, owner=이유석' && date && hostname",
    )

    done = BashOperator(
        task_id="done",
        bash_command="echo 'git-sync DAG 실행 완료'",
    )

    hello >> done
