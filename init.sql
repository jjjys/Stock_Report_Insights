/*
-- # 사용자 확인
\du

-- # stock 계정 삭제
DROP ROLE stock;

-- # stock 계정 생성
CREATE ROLE stock WITH LOGIN PASSWORD '비밀번호';
ALTER ROLE stock CREATEDB;

-- # db 확인 (데이터베이스 목록 확인)
\l
-- 또는
\list
-- SQL로 확인하고 싶다면:
SELECT datname, datdba FROM pg_database WHERE datistemplate = false;

-- # stockdb 삭제
DROP DATABASE stockdb;

-- # stockdb 생성
CREATE DATABASE stockdb OWNER stock;
*/

-- reports 테이블 (raw 데이터)
CREATE TABLE IF NOT EXISTS reports (
    id SERIAL PRIMARY KEY,
    post_date DATE NOT NULL, -- 레포트 포스트된 날짜
    report_name VARCHAR(255) NOT NULL UNIQUE,
    report_url TEXT NOT NULL UNIQUE, -- 링크 중복 방지
    report_preprocessed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- db 적재 날짜
);

-- LLM 테이블 (메타)
CREATE TABLE IF NOT EXISTS llm (
    llm_id SERIAL PRIMARY KEY,
    type VARCHAR(20) NOT NULL, -- e.g., 'gemini'
    version VARCHAR(20) NOT NULL,
    UNIQUE(type, version)
);

CREATE TABLE IF NOT EXISTS stock_info (
    stock_id SERIAL PRIMARY KEY,
    stock VARCHAR(100),
    ticker VARCHAR(6) NOT NULL UNIQUE CHECK (LENGTH(ticker) = 6), -- 다른 거래소 사용 시 변경
    UNIQUE(stock, ticker)
);

CREATE TABLE IF NOT EXISTS analyst (
    anal_id SERIAL PRIMARY KEY,
    name VARCHAR(15) NOT NULL,
    firm VARCHAR(100) NOT NULL,
    UNIQUE(name, firm)
);

-- report_extractions 테이블 (추출 결과)
CREATE TABLE IF NOT EXISTS report_extractions (
    id INTEGER REFERENCES reports(id) ON DELETE CASCADE, -- reports 삭제 시 연쇄 삭제 (옵션)
    llm_id INTEGER REFERENCES llm(llm_id) ON DELETE CASCADE,
    investment_opinion VARCHAR(4) CHECK (investment_opinion IN ('buy', 'hold', 'sell')), -- 구조화
    stock_id INTEGER REFERENCES stock_info(stock_id) ON DELETE SET NULL, -- stock 삭제 시 NULL (옵션)
    published_date DATE NOT NULL,
    current_price INTEGER CHECK (current_price > 0),
    target_price INTEGER NOT NULL CHECK (target_price > 0),
    analyst_id INTEGER REFERENCES analyst(anal_id) ON DELETE SET NULL,
    krx_loaded BOOLEAN DEFAULT FALSE,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, llm_id) -- 중복 추출 방지
);

-- KRX 테이블 (후속 분석)
CREATE TABLE IF NOT EXISTS krx (
    id INTEGER,
    llm_id INTEGER,
    PRIMARY KEY (id, llm_id),
    target_price_reached_date DATE NOT NULL,
    days_to_reach INTEGER NOT NULL, -- 도달까지 일수
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id, llm_id) REFERENCES report_extractions(id, llm_id) ON DELETE CASCADE -- composite FK 명시 (핵심 수정!)
);