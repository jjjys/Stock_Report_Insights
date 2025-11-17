# ──────────────────────────────
# 1. Base image
# ──────────────────────────────
FROM apache/airflow:2.10.2-python3.11

# ──────────────────────────────
# 2. 시스템 패키지 (Chrome + PDF 처리)
# ──────────────────────────────
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates \
        libglib2.0-0 libnss3 libgconf-2-4 \
        libx11-6 libx11-xcb1 libxcb1 \
        libxcomposite1 libxcursor1 libxdamage1 \
        libxi6 libxtst6 libxrandr2 \
        libasound2 libpangocairo-1.0-0 libatk1.0-0 \
        libgtk-3-0 libgbm1 libxshmfence1 \
        fonts-liberation && \
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# ──────────────────────────────
# 3. Python 의존성
# ──────────────────────────────
USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:airflow requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir google-generativeai

# ──────────────────────────────
# 4. 프로젝트 코드 복사
# ──────────────────────────────
COPY --chown=airflow:airflow . .

# ──────────────────────────────
# 5. Airflow 환경
# ──────────────────────────────
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__SECRET_KEY=change_me_in_prod