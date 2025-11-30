# 서버 올리기
docker-compose --profile db up --build

# 서버 내리기
docker-compose --profile db down --volumes --remove-orphans

# 레포트 수집(동적 웹 크롤링) 확인
1. docker exec -it stock_report_insights-airflow-worker-1 bash
2. ls data/reports/종목분석_리포트/
3. ls data/reports_processed/

# 데이터 베이스(psql) 확인
1. docker exec -it stock_report_insights-postgres-1 bash
2. psql -U postgres -d stockdb
3. SQL 문으로 확인
3.1. SELECT * FROM reports;
3.2. SELECT * FROM report_extractions;
3.3. SELECT * FROM krx;

# Git Commit message rules
1. feat: 새로운 기능 추가
2. fix: 버그 수정
3. docs: 문서/README/주석
4. refactor: 코드 리팩토링 (동작 변화 없음)
5. chore: 빌드, 의존성, 설정 등 기타 작업

# 환경
Window11
Python3.10

# 웹 크롤링 작업 수정
1. 기존: SeleniumBase 프레임 워크
2. 수정: Undected-Chrome 프레임 워크

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
