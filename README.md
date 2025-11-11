# Git Commit message rules
1. feat: 새로운 기능 추가
2. fix: 버그 수정
3. docs: 문서/README/주석
4. refactor: 코드 리팩토링 (동작 변화 없음)
5. chore: 빌드, 의존성, 설정 등 기타 작업

# 프로젝트 구조
<pre>
Stock_Report_Insights/
├── README.md                 # 프로젝트 설명, 설치/실행 가이드
├── requirements.txt          # Python 의존성 (seleniumbase, google-generativeai, sqlalchemy 등)
├── .gitignore                # git 무시 파일 (data/, logs/, __pycache__ 등)
├── .env                      # 환경 변수 템플릿 (DB_PASS, GEMINI_API_KEY 등)
├── docker-compose.yml        # Docker 서비스 정의 (Airflow, PostgreSQL, Redis)
├── init.sql                  # PostgreSQL 초기 스키마 생성 스크립트
│
├── src/                      # 메인 소스 코드 (Python 모듈)
│   ├── __init__.py
│   ├── crawlers/             # 데이터 추출 모듈
│   │   ├── __init__.py
│   │   ├── naver_crawler.py  # 기존 main_crawl.py 확장 (PDF 수집)
│   │   └── krx_extractor.py  # KRX API/크롤링 로직
│   ├── extractors/           # LLM 추출 모듈
│   │   ├── __init__.py
│   │   └── llm_extractor.py  # PDF 텍스트 추출 + Gemini API 호출
│   ├── loaders/              # DB 저장 모듈
│   │   ├── __init__.py
│   │   └── pg_naver_loader.py  # SQLAlchemy로 INSERT/UPDATE
│   │   └── pg_krx_loader.py  # SQLAlchemy로 INSERT/UPDATE
│   │   └── pg_llm_loader.py  # SQLAlchemy로 INSERT/UPDATE
│   ├── utils/                # 공통 유틸리티
│   │   ├── __init__.py
│   │   ├── logger.py         # 로깅 설정
│   │   ├── notifier.py       # Discord 알림 (discord.py 사용)
│   │   └── nodes
│   │       ├── __init__.py
│   │       ├── cores.py
│   │       ├── database.py
│   │       ├── scheme.py
|   |       └── users.py
│   │
│   └── main.py               # CLI 엔트리포인트 (Airflow 외 수동 실행용)
│
├── dags/                     # Airflow DAG 파일들
│   ├── __init__.py
│   └── securities_etl_dag.py # 메인 DAG (태스크 연결: crawl → extract → load 등)
│
├── config/                   # 설정 파일 (비코드)
│   ├── airflow.cfg          # Airflow 설정 (옵션, Docker에서 오버라이드)
│   └── prompts/              # LLM 프롬프트 템플릿
│       └── report_extraction_prompt.txt  # "타겟 가격, 추천 등 추출" 템플릿
│
├── data/                     # 데이터 저장 (git 무시)
│   ├── raw/                  # 원본 PDF/JSON
│   │   ├── pdfs/             # 카테고리별 PDF 폴더 (시황정보/, 종목분석/ 등)
│   │   └── checkpoints/      # 크롤링 체크포인트 JSON
│   └── processed/            # LLM 추출 후 JSON (임시, DB로 이동)
│
├── logs/                     # 로그 파일 (git 무시)
│   └── airflow/              # Airflow 로그
│
└── scripts/                  # 유틸 스크립트
    ├── setup.sh              # 초기 설정 (pip install, DB init)
    └── migrate.sh            # Alembic 마이그레이션 (스키마 변경 시)
</pre>

# Naver_Securities_Report
네이버 증권 리포트 pdf 파일 데이터 수집 후 GEMINI 활용하여 PDF 분석하여 투자 전략 구현


파이썬 3.10 버전으로 다시 진행.
1. pip install seleniumbase
2. 코드 제대로 수행 확인
3. pip install -q -U google-genai
4. gemini 공식 문서 확인 및 개발


# 최대 페이지 제한 설정
end_page = 999 

# 참고 자료
1. https://aistudio.google.com/apikey
2. https://ai.google.dev/gemini-api/docs/quickstart
3. https://ai.google.dev/gemini-api/docs/document-processing
4. https://ai.google.dev/gemini-api/docs/image-understanding
5. https://docs.ollama.com/quickstart
6. https://docs.ollama.com/capabilities/embeddings
