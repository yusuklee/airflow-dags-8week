
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import boto3
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup



COLLECTOR = "이유석"
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
LAWD_CODES = ["11680", "11650", "11710", "11440", "11170", "11200"]
S3_BUCKET = os.environ.get("S3_BUCKET", "realestate-leeyuseok")
TMP_DIR = "/tmp/bronze"


def local_path(ym,lawd):
    return f"{TMP_DIR}/{ym}/{lawd}.xml"


def collect(lawd, **dates):
    print(f"collector={COLLECTOR}, time={datetime.now()}, lawd={lawd}")
    start_date = dates["data_interval_start"].strftime("%Y%m")
    resp = requests.get(
        API_URL,
        params={
            "serviceKey":os.environ["DATA_GO_KR_API_KEY"],
            "LAWD_CD":lawd,
            "DEAL_YMD":start_date,
            "pageNo":1,
            "numOfRows":1000,
        },
        timeout=30,
    )
    resp.raise_for_status()

    p=local_path(start_date, lawd)
    os.makedirs(os.path.dirname(p),exist_ok=True)
    with open(p,"wb") as f:
        f.write(resp.content)

def chk_resp(lawd, **dates):
    start_date = dates["data_interval_start"].strftime("%Y%m")
    group = f"collect_{lawd}"
    try:
        root = ET.parse(local_path(start_date, lawd)).getroot()
    except ET.ParseError as e:
        print(f"파싱 실패 {e} ")
        return f"{group}.skip"

    items = root.findall(".//item")
    print(f"lawd={lawd}, start_date={start_date}, itmes={len(items)}")
    if not items:
        return f"{group}.skip"
    return f"{group}.upload_s3"

def upload_s3(lawd, **dates):
    start_date = dates["data_interval_start"].strftime("%Y%m")
    key= f"bronze/{start_date}/{lawd}.xml"
    with open(local_path(start_date,lawd), "rb") as f:
        boto3.client("s3").put_object(
            Bucket=S3_BUCKET, Key=key, Body=f.read(), ContentType="application/xml"
        )
    print(f"uploaded: s3://{S3_BUCKET}/{key}")

with DAG(
    dag_id="bronze_realestate_collect",
    schedule="@monthly",
    start_date=datetime(2026,7,1),
    catchup=True,
    max_active_runs=1,
    default_args={"retries":2, "retry_delay":timedelta(minutes=1)},
    tags=["realestate","bronze"],
) as dag:
    start = EmptyOperator(task_id="start")
    end=EmptyOperator(task_id="end",trigger_rule="none_failed_min_one_success")

    for lawd in LAWD_CODES:
        with TaskGroup(group_id=f"collect_{lawd}") as tg:
            collect_task= PythonOperator(
                task_id="collect",
                python_callable=collect,
                op_kwargs={"lawd":lawd},
            )
            branch_task= BranchPythonOperator(
                task_id="check_response",
                python_callable=chk_resp,
                op_kwargs={"lawd":lawd},
            )
            upload_task=PythonOperator(
                task_id="upload_s3",
                python_callable=upload_s3,
                op_kwargs={"lawd":lawd},
            )
            skip_task = EmptyOperator(task_id="skip")

            collect_task >> branch_task >> [upload_task, skip_task]

        start >> tg >> end

