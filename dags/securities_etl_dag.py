from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
import json
from main import load_raw_reports, load_report_feats, load_krx_hit

def t1_dag(**context):
    load_raw_reports()

def t2_dag(**context):
    load_report_feats()

def t3_dag(**context):
    load_krx_hit()

# DAG 정의
with DAG(
    'test',
    description='test',
    # schedule='0 23 * * *',
    start_date=datetime(2025, 11, 14),
    catchup=False
) as dag:
    
    t1 = PythonOperator(
        task_id='load_raw_reports',
        python_callable=t1_dag,
        # op_kwargs=[]
    )
    
    t2 = PythonOperator(
        task_id='load_report_feats',
        python_callable=t2_dag,
        op_args=["gemini", "2.5-flash"]
    )
    
    t3 = PythonOperator(
        task_id='load_krx_hit',
        python_callable=t3_dag
    )
    
    # 태스크 의존성: t1 -> t2 -> t3 -> t4 -> t5
    t1 >> t2 >> t3 