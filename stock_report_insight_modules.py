"""stock_report_insight_refactor.ipynb"""


# 실행 환경
from dotenv import load_dotenv
import yaml

# 모델
from google import genai

import numpy as np
import pandas as pd

from datetime import datetime, timedelta
import os, subprocess, time, json, shutil
import warnings

# 병렬처리
from concurrent.futures import ThreadPoolExecutor, as_completed

import traceback
import psycopg2
from pykrx import stock

# DB 스키마
from dataclasses import dataclass, field
from typing import List, Optional
from multipledispatch import dispatch

load_dotenv()
postgres_key = os.getenv('POSTGRES_KEY')
google_api_key = os.getenv('GOOGLE_API_KEY')

report_path = '/content/drive/My Drive/이어드림/reports'
report_finished_path = '/content/drive/My Drive/이어드림/reports_finished'

if os.path.exists(report_path):
    print(f"디렉터리 '{report_path}'가 존재합니다.")
    # print("디렉터리 내용:")
    # for item in os.listdir(report_path):
    #     print(item)
else:
    print(f"디렉터리 '{report_path}'가 존재하지 않습니다.")

system_prompt = """
You are a highly skilled information extraction bot.
Your task is to extract specific information from the provided securities report PDF file.
Extract the following details and return them in JSON format:

- stock (종목명, Stock Name)
- ticker (종목코드/티커, Stock Code/Ticker)
- published_date (리포트 작성일, Date of Report)
- current_price (현재 주가, Current Stock Price - only numeric, positive integer value)
- target_price (목표 주가, Target Stock Price - only numeric, positive integer value)
- investment_opinion (투자 의견 - only in "Buy", "Hold" or "Sell")
- author (작성 애널리스트, Author Analyst)
- firm (소속 증권사, Affiliated Securities Firm)

If a piece of information is not found, use 'N/A' for string values and 0 for numeric values.
(0 means: Not Found)

Return only the JSON object. Do not include any other text.

Example JSON format:
{{
  "stock": "예시 종목명",
  "ticker": "000000",
  "published_date": "YYYY-MM-DD",
  "current_price": 10000,
  "target_price": 12000,
  "investment_opinion": "BUY",
  "author": "애널리스트 이름",
  "firm": "증권사명"
}}
"""


# ------------------------------
# 공통 베이스 클래스 정의
# ------------------------------
class Node:
    """모든 노드의 공통 부모 클래스. 연산자로 연결 가능."""

    def __or__(self, other):
        # self 다음에 other를 실행하는 새 파이프라인을 반환
        return Pipeline([self, other])

    def __add__(self, other):
        """self와 other를 하나의 노드로 연결하여 동시에 실행"""
        return Combined(self, other)

    def __mul__(self, workers):
        """멀티스레딩을 쉽게 적용할 수 있는 연산자"""
        return MultiThreadNode(self, max_workers=workers)

    def __call__(self, data):
        """각 노드가 수행할 구체적 처리 로직 (자식 클래스에서 구현)"""
        raise NotImplementedError


class Combined(Node):
    """두 노드를 동시에 실행 (추출 + DB 적재)"""
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __call__(self, data):
        result = self.left(data)
        self.right(result)
        return result  # 필요시 결과를 반환


class MultiThreadNode(Node):
    """내부 노드를 멀티스레딩으로 실행하는 노드"""
    def __init__(self, node, max_workers:int=4):
        self.node = node
        self.max_workers = max_workers

    def __call__(self, data_list:list|tuple):
        """data_list: 여러 입력 데이터를 동시에 처리"""
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.node, d): d for d in data_list}

            for future in as_completed(futures):
                data_item = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    print(f"[MultiThreadNode] '{data_item}' 처리 중 오류 발생: {e}")
                    traceback.print_exc(limit=1)
                    result = None  # 실패한 항목은 None 처리
                results.append(result)

        print("[MultiThreadNode] 모든 작업 완료")
        return results


class MapNode(Node):
    """리스트 형태의 입력을 받아 내부 노드를 각 항목별로 실행"""
    def __init__(self, node):
        self.node = node

    def __call__(self, data_list:list|tuple):
        if not isinstance(data_list, (list, tuple)):
            raise TypeError("MapNode는 리스트 형태의 입력만 처리할 수 있습니다.")

        print(f"[MapNode] {len(data_list)}개의 항목을 순차 처리 중...")
        results = []
        for i, d in enumerate(data_list, start=1):
            try:
                res = self.node(d)
                results.append(res)
            except Exception as e:
                print(f"[MapNode] {i}번째 항목 처리 중 오류 발생: {e}")
                results.append(None)

        print("[MapNode] 모든 작업 완료")
        return results


class Pipeline(Node):
    """여러 노드를 순차적으로 연결해 실행하는 클래스"""
    def __init__(self, nodes):
        self.nodes = []
        # 파이프라인 합성 지원
        for n in nodes:
            if isinstance(n, Pipeline):
                self.nodes.extend(n.nodes)
            else:
                self.nodes.append(n)

    def __or__(self, other):
        # Pipeline | Node 형태의 연결 지원
        return Pipeline(self.nodes + [other])

    def __call__(self, data):
        """파이프라인 실행"""
        value = data
        for node in self.nodes:
            value = node(value)
        return value


# --------------------------
# 데이터 구조 정의
# --------------------------
@dataclass
class ColumnDef:
    name: str
    dtype: str
    primary_key: bool = False
    foreign_key: list | tuple = None # (table, column)
    nullable: bool = True
    check: str = None   # syntax valid
    default: Optional[str] = None

@dataclass
class TableDef:
    name: str
    columns: List[ColumnDef] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)  # syntax valid


# --------------------------
# YAML 로딩 헬퍼
# --------------------------
"""
tables:
  - name: orders
    columns:
      - name: order_id
        dtype: INT
        primary_key: true
        nullable: false
      - name: user_id
        dtype: INT
        nullable: false
        foreign_key: ["users", "id"]
      - name: order_date
        dtype: DATE
    constraints:
      - "UNIQUE (order_id, user_id)"
      - "CHECK (order_date <= CURRENT_DATE)"
"""
def load_schema_from_yaml(file_path: str) -> List[TableDef]:
    with open(file_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    tables = []
    for t in raw.get("tables", []):
        columns = [ColumnDef(**col) for col in t.get("columns", [])]
        tables.append(TableDef(name=t["name"], columns=columns, constraints=t.get("constraints", [])))  # name 필수, cons 선택

    return tables


class DBNode(Node):
    """
    DB 연결 노드는 DBNode 상속하여 생성자 통해 conn, cursor 객체 자동 생성.
    생성자 오버라이드 시 반드시 conn, cursor 객체 생성 필요.
    """
    def __init__(self, conn=None, cursor=None, db_key:str=None):
        if conn is not None:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(host="localhost", dbname="stockdb", user="stock", password=db_key)
            self.cursor = self.conn.cursor()
            

# ------------------------------
# 각 노드 정의 및 구현
# ------------------------------
class DocumentsLoader(Node):
    def __init__(self, docs_dir_path:str=None, extension:str=None, verbose:bool=False):
        """
        Args:
            docs_dir_path (str): 참고문서 파일이 있는 디렉토리 경로
            extension (str): 참고문서 파일 확장자
            verbose (bool): 진행상황 파악 여부
        """
        self.docs_dir_path = docs_dir_path
        self.extension = extension if extension is not None else "PDF"
        self.verbose = verbose

    def __call__(self, *args, **kwargs) -> list:
        """
        경로에서 참고문서 파일 목록을 가져옴
        
        Returns: 참고문서 파일명 (list)
        """
        print(f"[Files Loader] 디렉토리 경로: {self.docs_dir_path}")
        all_items = os.listdir(self.docs_dir_path)
        doc_files = [item for item in all_items if item.endswith("".join([".", self.extension.lower()]))]

        if self.verbose:
          print(f"다음 {self.extension.upper()} 파일이 로드됩니다:")
          for file in doc_files:
              print(file)

        return doc_files

class LLMFeatsExtractor(Node):
    def __init__(self, docs_dir_path:str, llm_type:str, llm_version:str, prompt:str, interval:int|float=0, api_key:str=None, essential_cols:list|tuple=None):
        """
        Args:
            docs_dir_path (str): 참고문서 디렉토리 경로 (DocumentsLoader 사용 시 경로 일치 필수)
            llm_version (str): 생성자 LLM 버전 정보 - 포맷은 모델에 따라 다름 (공식문서 참조)
            prompt (str): LLM에 전달할 프롬프트
            interval (int|float): LLM API 호출 간격 (초)
            api_key (str): LLM API 키 (로컬 모델의 경우 필요 없음)
            essential_cols (list|tuple): 추출 항목 중 필수 항목명
        """
        self.docs_dir_path = docs_dir_path
        self.llm_type = llm_type
        self.llm_version = llm_version
        self.prompt = prompt
        self.interval = interval
        self.api_key = api_key
        self.essential_cols = essential_cols if essential_cols is not None else tuple()

        self.model_map = {"gemini": self.call_gemini,}# "llama": self.call_llama, "qwen": self.call_qwen} # 모델명 + 메소드 매핑
        self.na_items = (None, "N/A", "n/a", "", 0) # 추출 실패 시 발생 항목

    def __call__(self, doc:str, *args, **kwargs) -> dict:
        """
        단일 참고문서에서 LLM을 통해 필요한 정보를 추출

        Args:
            doc (str): 단일 참고문서 파일명
        Returns: 참고문서에서 추출된 정보 (dict)
        """
        print(f"[LLMFeatsExtractor] {self.llm_type}(으)로 데이터 추출 중...")
        extractor = self.model_map.get(self.llm_type, None)
        file_path = os.path.join(self.docs_dir_path, doc)

        if extractor is None:
            raise ValueError(f"지원하지 않는 LLM 타입: {self.llm_type}")

        if self.interval > 0:
            time.sleep(self.interval)

        try:
            response = extractor(file_path, self.llm_version, self.prompt, self.prompt, self.api_key)

            if self.is_valid_response(response):
                return response
            else:
                raise ValueError(f"{os.path.basename(file_path)} 필수 데이터 없음: {response}")

        except Exception as e:
            print(f"[LLMFeatsExtractor] {self.llm_type}-{self.llm_version} Error: {e}")

    def is_valid_response(self, response:dict) -> bool:
        is_valid = response is not None and isinstance(response, dict)

        for ec in self.essential_cols:
            is_valid = is_valid and response.get(ec) not in self.na_items

        return is_valid

    def call_gemini(self, file_path:str, llm_version:str, prompt:str, interval:int|float=0, api_key:str=None) -> dict:
        client = genai.Client(api_key=api_key)
        sample_file = client.files.upload(file=file_path)

        response = client.models.generate_content(model=f"gemini-{llm_version}", contents=[sample_file, prompt])
        response = response.text.replace("```json", "").replace("```", "").strip()

        return json.loads(response)

    def call_llama(self) -> dict:
        pass

    def call_qwen(self) -> dict:
        pass

class DataProcessor(Node):
    def __call__(self, data, *args, **kwargs):
        print("[DataProcessor] 사용자 정의 노드 구현용...")
        raise NotImplementedError

class KrxTargetHitter(Node):
    def __init__(self, ticker_key:str=None, report_date_key:str=None, target_price_key:str=None):
        """
        파이프라인 내 노드로 사용 시 반드시 작성
        
        Args:
            ticker_key (str): [LLMFeatsExtractor] 종목코드 key
            report_date_key (str): [LLMFeatsExtractor] 작성일 key
            target_price_key (str): [LLMFeatsExtractor] 목표 주가 key
        """
        self.ticker_key = ticker_key
        self.report_date_key = report_date_key
        self.target_price_key = target_price_key

    @dispatch(dict)
    def __call__(self, trt:dict) -> tuple[str, timedelta]:
        # 파이프라인 내 노드로 사용
        trt = {"ticker": trt.get(self.ticker_key, None),
               "report_date": trt.get(self.report_date_key, None),
               "target_price": trt.get(self.target_price_key, None)}

        return self.krx_target_hitter(**trt)
    
    @dispatch(str, str, int)
    def __call__(self, ticker:str, report_date:str, target_price:int) -> tuple[str, timedelta]:
        # 직접 호출 시 사용
        return self.krx_target_hitter(ticker, report_date, target_price)

    def krx_target_hitter(self, ticker:str, report_date:str, target_price:int) -> tuple[str, timedelta]:
        start_date = report_date.replace("-", "")
        end_date = datetime.today().strftime("%Y%m%d")

        print(f"[KrxTargetHitter] KRX 데이터 호출 중...")
        df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
        df = df[["종가"]]

        reached = df[df["종가"] >= target_price]

        if not reached.empty:
            first_hit_date = reached.index[0].strftime("%Y-%m-%d")

            report_dt = datetime.strptime(report_date, "%Y-%m-%d")
            hit_dt = datetime.strptime(first_hit_date, "%Y-%m-%d")

            return first_hit_date, (hit_dt - report_dt).days
        else:
            return None, None

class DBWriter(DBNode):
    def __init__(self, table:str, pk:list[str]|tuple[str], do_upsert:bool=False, toss_input:bool=False):
        """
        Args:
            table (str): 테이블명
            pk (list[str]|tuple[str]): 기본키 컬럼 리스트 (실제 스키마와 일치 권장)
            do_upsert (bool): Upsert 여부
            toss_input (bool): 입력 데이터 출력 여부 (False 시 출력은 None)
        """
        self.table = table
        self.pk = pk
        self.do_upsert = do_upsert
        self.toss_input = toss_input

    def __call__(self, data:dict, *args, **kwargs) -> dict:
        """
        단일 데이터 DB INSERT

        Args:
            data (dict): 데이터 = {'컬럼명': 값} (컬럼명, 데이터 타입 실제 스키마와 일치 필수)
        Returns: 
            - 입력 데이터 (toss_input=False)
            - None (toss_input=True)
        """
        print(f"[DBWriter] 데이터 삽입: {data}")
        try:
            conflict_action = None
            if self.do_upsert:
                set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in data.keys() if col not in self.pk])
                conflict_action = f"UPDATE SET {set_clause}"
            else:
                conflict_action = "NOTHING"

            self.cursor.execute(f"""
                INSERT INTO { self.table } ({ ", ".join(data.keys()) })
                VALUES ({ ", ".join(["%s"] * len(data)) })
                ON CONFLICT ({ ", ".join(self.pk) }) DO { conflict_action };
            """, tuple(data.values()))

        except (Exception, psycopg2.Error) as e:
            self.conn.rollback()
            print(f"[DBWriter] DB INSERT Error: {e}")
        else:
            self.conn.commit()
            print("DB INSERT Success")
        finally:
            self.conn.close() # 노드별 책임 분리
            return data if self.toss_input else None

class DBSelector(DBNode):
    def __init__(self, table:str, cols:list[str]|tuple[str], conditions:dict):
        """
        Args:
            table (str): 테이블명
            cols (list[str]|tuple[str]): 컬럼명 리스트
            conditions (dict): {조건 컬럼: 조건 문법} (e.g. {'when': 'BETWEEN .. AND ..'}) # syntax valid
        """
        self.table = table
        self.cols = cols
        self.conditions = conditions

    def __call__(self, *args, **kwargs) -> list[tuple]:
        """
        단일 테이블 DB SELECT
        
        Returns: 조회 결과 (list)
        """
        print("[DBSelector] 데이터 조회")
        self.cursor.execute(f"""
            SELECT { ", ".join(self.cols) } FROM { self.table }
            WHERE { " AND ".join([f"{k} {v}" for k, v in self.conditions.items()]) };
        """)

        result = self.cursor.fetchall()
        self.conn.close()

        return result

class Schematizer(Node):
    def __init__(self, db_key:str):
        # 데이터베이스 연결
        self.conn = psycopg2.connect(host="localhost", dbname="stockdb", user="stock", password=db_key)
        self.cursor = self.conn.cursor()

    def __call__(self, tables: List[TableDef]):
        """
        YAML 파일의 DB 스키마 정보 반영하여 테이블 확인 및 생성
        Args:
            tables (List[TableDef]): 테이블 정의 리스트
        """
        try:
            for table in tables:
                self.create_table_if_not_exists(table)
        except (Exception, psycopg2.Error) as e:
            self.conn.rollback()
            self.conn.close()
            print(f"[Schematizer] DB CREATE Error: {e}")
            raise # 스키마 단의 에러 발생 시 전체 파이프라인 중지
        else:
            self.conn.commit()
            self.conn.close()

    def create_table_if_not_exists(self, table:TableDef):
        col_defs = []
        for col in table.columns:
            col_def = f"{col.name} {col.dtype}"
            if col.primary_key:
                col_def += " PRIMARY KEY"
            if col.foreign_key is not None:
                col_def += f" REFERENCES {col.foreign_key[0]}({col.foreign_key[1]})"
            if not col.nullable:
                col_def += " NOT NULL"
            if col.check is not None:
                col_def += f" CHECK ({col.check})"
            if col.default is not None:
                col_def += f" DEFAULT {col.default}"
            col_defs.append(col_def)
        col_defs.extend(table.constraints)

        sql = f"CREATE TABLE IF NOT EXISTS {table.name} ({', '.join(col_defs)});"
        print(f"[Schematizer] 실행 SQL:\n{sql}")
        self.cursor.execute(sql)


# ------------------------------
# 사용 예시
# ------------------------------
"""
if __name__ == "__main__":
    pipeline = (reports_getter | MapNode(extractor | test_print | krx_hitter | test_print))
    result = pipeline(None)
    print("최종 결과:", result)
"""
