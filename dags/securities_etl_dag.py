from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main import load_raw_reports, load_report_feats, load_krx_hit, file_discord

def t1_dag(start_date, end_date, **context):
    """Raw reports 수집"""
    load_raw_reports(start_date=start_date, end_date=end_date)

def t1_1_dag():
    """Discord 백업"""
    file_discord()

def t2_dag(**context):
    """LLM 특성 추출"""
    load_report_feats(context["llm_type"], context["llm_version"])

def t3_dag():
    """KRX 목표주가 추출"""
    load_krx_hit()

# DAG 정의
with DAG(
    'Stock_Report_Insights',
    description='Stock_Report_Insights',
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=["Stock_Report_Insights"]
) as dag:
    
    t1 = PythonOperator(
        task_id='reports_download_load',
        python_callable=t1_dag,
        op_kwargs={
            "start_date": "2023-02-16",  # 시작 날짜 지정
            "end_date": "2023-02-16"     # 종료 날짜 지정
        }
    )

    t1_1 = PythonOperator(
        task_id='discord_backup',
        python_callable=t1_1_dag
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
    
    # 태스크 의존성
    t1 >> [t1_1, t2]
    t2 >> t3