from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from main import load_raw_reports, load_report_feats, load_krx_hit

def t1_dag(**context):
    load_raw_reports(context["prev_ds"], context["ds"])

def t2_dag(**context):
    load_report_feats(context["llm_type"], context["llm_version"])

def t3_dag():
    load_krx_hit()

# DAG 정의
with DAG(
    'test',
    description='test',
    # schedule='0 23 * * *',
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=["test"]
) as dag:
    
    t1 = PythonOperator(
        task_id='reports_download_load',
        python_callable=t1_dag
    )
    
    t2 = PythonOperator(
        task_id='extract_report_items_load',
        python_callable=t2_dag,
        op_kwargs={"llm_type": "gemini", "llm_version": "2.5-flash"}
    )
    
    t3 = PythonOperator(
        task_id='get_krx_hit_load',
        python_callable=t3_dag
    )
    
    # 태스크 의존성: t1 -> t2 -> t3 -> t4 -> t5
    t1 >> t2 >> t3 