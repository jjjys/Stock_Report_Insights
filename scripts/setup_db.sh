# scripts/setup_db.sh (최종 통합 버전)
#!/bin/bash
set -e

# === 환경 변수 로드 (.env 또는 docker-compose env_file) ===
DB_HOST="${DB_HOST:-postgres}"        # 내부: postgres, 외부: localhost
DB_PORT="${DB_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-airflow}"         # superuser
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-airflow}"       # super password
POSTGRES_DB="${POSTGRES_DB:-airflow}"
DB_USER="${DB_USER:-stock}"
DB_NAME="${DB_NAME:-stockdb}"
USER_KEY="${USER_KEY:-$POSTGRES_KEY}"

WAIT_TIMEOUT=${WAIT_TIMEOUT:-10}

echo "DB Setup: $DB_HOST:$DB_PORT → $DB_NAME (user: $DB_USER)"

# === pg_isready 대기 (외부 DB 필수) ===
start=$(date +%s)
echo "Waiting for PostgreSQL ($DB_HOST:$DB_PORT) – timeout: ${WAIT_TIMEOUT}s"

while true; do
    if pg_isready -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER >/dev/null 2>&1; then
        echo "PostgreSQL is ready!"
        break
    fi

    elapsed=$(( $(date +%s) - start ))
    if (( elapsed > WAIT_TIMEOUT )); then
        echo "ERROR: Connection timeout after ${WAIT_TIMEOUT}s"
        if [[ "$DB_HOST" = "postgres" ]]; then
            echo "   → Did you forget '--profile db'?"
        fi
        exit 1
    fi

    echo "   retrying... (${elapsed}s / ${WAIT_TIMEOUT}s)"
    sleep 2
done

# === psql 명령어 정의 ===
export PGPASSWORD="$POSTGRES_PASSWORD"
PSQL_SUPER="psql -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER -v ON_ERROR_STOP=1"
PSQL_DBUSER="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -v ON_ERROR_STOP=1"

# === 사용자 검증 === # 권한 분리 필요 시 실행
# if [ "$POSTGRES_USER" = "$DB_USER" ]; then
#     echo "ERROR: POSTGRES_USER and DB_USER cannot be the same."
#     exit 1
# fi

# === 사용자 생성 ===
if ! $PSQL -tAc "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" | grep -q 1; then
    echo "Creating user: $DB_USER"
    $PSQL_SUPER -c "CREATE USER $DB_USER WITH PASSWORD '$USER_KEY' LOGIN;"
    # $PSQL -c "ALTER USER $DB_USER CREATEDB;"
else
    echo "User exists: $DB_USER"
fi

# === 데이터베이스 생성 ===
if ! $PSQL -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Creating database: $DB_NAME"
    $PSQL_SUPER -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
else
    echo "Database exists: $DB_NAME"
fi

# === 권한 부여 ===
echo "Applying default privileges..."
$PSQL_SUPER -d $DB_NAME -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;"
$PSQL_SUPER -d $DB_NAME -c "GRANT USAGE ON SCHEMA public TO $DB_USER;"
$PSQL_SUPER -d $DB_NAME -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $DB_USER;"
$PSQL_SUPER -d $DB_NAME -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $DB_USER;"

# === init.sql 실행 ===
echo "Applying schema from init.sql..."
export PGPASSWORD="$USER_KEY"
$PSQL_DBUSER -d $DB_NAME -f /opt/airflow/init.sql

# ========== init.sql로 생성된 테이블의 owner 보정 ==========
echo "Fixing ownership of all tables to $DB_USER..."

export PGPASSWORD="$POSTGRES_PASSWORD"
$PSQL_SUPER -d $DB_NAME -c "
DO \$\$
DECLARE r RECORD;
BEGIN
    FOR r IN (
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname='public'
    )
    LOOP
        EXECUTE 'ALTER TABLE public.' || quote_ident(r.tablename) || ' OWNER TO $DB_USER';
    END LOOP;
END
\$\$;
"

# ========== 시퀀스 권한 (serial, identity 컬럼 대비) ==========
echo "Granting sequence privileges..."
$PSQL_SUPER -d $DB_NAME -c "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO $DB_USER;"

echo "DB setup completed!"