# 실행 환경, 오버라이딩
from dotenv import load_dotenv
# from multipledispatch import dispatch

# 병렬처리
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml, traceback, os
from dataclasses import dataclass, field
from typing import List, Optional

# DB Connect
import psycopg2

load_dotenv()

# ------------------------------
# 공통 베이스 클래스 정의
# ------------------------------
class Node:
    """모든 노드의 공통 부모 클래스. 연산자로 연결 가능."""

    def __or__(self, other):
        # self 다음에 other를 실행하는 새 파이프라인을 반환
        return Pipeline([self, other])

    def __sub__(self, other):
        """self와 other를 하나의 노드로 연결하여 동시에 실행. 앞 노드 결과만 반환."""
        return Combined(self, other, hop_mode=True)

    def __add__(self, other):
        """self와 other를 하나의 노드로 연결하여 동시에 실행. 앞뒤 노드 결과 모두 반환."""
        return Combined(self, other, hop_mode=False)

    def __mul__(self, workers):
        """멀티스레딩을 쉽게 적용할 수 있는 연산자"""
        return MultiThreadNode(self, max_workers=workers)

    def __call__(self, data):
        """각 노드가 수행할 구체적 처리 로직 (자식 클래스에서 구현)"""
        raise NotImplementedError


class Combined(Node):
    """두 노드를 동시에 실행 (추출 + DB 적재)"""
    def __init__(self, left, right, hop_mode:bool):
        self.left = left
        self.right = right
        self.hop_mode = hop_mode

    def __call__(self, data):
        result_l = self.left(data)
        result_r = self.right(result_l)

        if self.hop_mode:
            return result_l  # 필요시 결과를 반환
        else:
            return result_l, result_r


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
# 범용 노드
# --------------------------
class DataProcessor(Node):
    def __call__(self, data, *args, **kwargs):
        print("[DataProcessor] 사용자 정의 노드 구현용...")
        raise NotImplementedError
    

# --------------------------
# DB 연결 노드
# --------------------------
class DBNode(Node):
    """
    DB 연결 노드는 DBNode 상속하여 생성자 통해 conn, cursor 객체 자동 생성.
    생성자 오버라이드 시 반드시 conn, cursor 객체 생성 필요.
    호출 함수(__call__)는 오버라이드 권장.
    """
    def __init__(self, conn=None, cursor=None):
        self.inner_conn = conn is None

        if not self.inner_conn:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(host="localhost", dbname="stockdb", user="stock", password=os.getenv('POSTGRES_KEY'))
            self.cursor = self.conn.cursor()

    def __call__(self, query:str, *args, **kwargs) -> list[tuple]:
        # 파이프라인 내 노드로 사용
        return self.call_db_with_complete_query(query)

    def call_db_with_complete_query(self, query:str, data:list|tuple=None) -> list[tuple]:
        """
        직접 호출 시 사용 (상속 시 사용하지 않음 권장)

        Args:
            query (str): 쿼리문 (데이터 동적 제공 시 %s 사용)
            data (list|tuple): 쿼리 데이터 (동적 제공 시 사용)
        """
        try:
            print(f"[DBNode] 실행 SQL:\n{query}")
            self.cursor.execute(query, data)
        except (Exception, psycopg2.Error) as e:
            self.conn.rollback()
            print(f"[DBNode] Error: {e}")
        else:
            self.conn.commit()
            print("[DBNode] SQL Success")
        finally:
            if self.inner_conn:
                self.conn.close()

            if "select" in query.lower() and "from" in query.lower():
                return self.cursor.fetchall()
    

class DBWriter(DBNode):
    def __init__(self, table:str, pk:list[str]|tuple[str], do_upsert:bool=False, toss_input:bool=False, conn=None, cursor=None):
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

        self.inner_conn = conn is None
        if not self.inner_conn:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(host="localhost", dbname="stockdb", user="stock", password=os.getenv('POSTGRES_KEY'))
            self.cursor = self.conn.cursor()

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
            if self.inner_conn:
                self.conn.close() # 노드별 책임 분리
            return data if self.toss_input else None


class DBSelector(DBNode):
    def __init__(self, table:str, cols:list[str]|tuple[str]=None, conn=None, cursor=None):
        """
        Args:
            table (str): 테이블명
            cols (list[str]|tuple[str]): 컬럼명 리스트
        """
        self.table = table
        self.cols = cols

        self.inner_conn = conn is None
        if not self.inner_conn:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(host="localhost", dbname="stockdb", user="stock", password=os.getenv('POSTGRES_KEY'))
            self.cursor = self.conn.cursor()

    def __call__(self, conditions:dict, *args, **kwargs) -> list[tuple]:  # WHERE 구문을 파이프라인 내 사용 어려움
        """
        단일 테이블 DB SELECT

        Args:
            conditions (dict): {조건 컬럼: 조건 문법} (e.g. {'when': 'BETWEEN .. AND ..'}) # syntax valid
        Returns: 조회 결과 (list)
        """
        print("[DBSelector] 데이터 조회")
        self.cursor.execute(f"""
            SELECT { ", ".join(self.cols) if self.cols is not None else "*" } FROM { self.table }
            WHERE { " AND ".join([f"{k} {v}" for k, v in conditions.items()]) };
        """)

        result = self.cursor.fetchall()
        
        if self.inner_conn:
            self.conn.close()
        return result
    

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
    check: str = None               # syntax valid
    default: Optional[str] = None

@dataclass
class TableDef:
    name: str
    columns: List[ColumnDef] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)  # syntax valid


class Schematizer(DBNode):
    def __call__(self, tables: List[TableDef]):
        """
        YAML 파일의 DB 스키마 정보 반영하여 테이블 확인 및 생성.
        파이프라인 시작 시 한 번만 실행.

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


# --------------------------
# YAML 로딩 헬퍼
# --------------------------
def load_schema_from_yaml(file_path: str) -> List[TableDef]:
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
    with open(file_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    tables = []
    for t in raw.get("tables", []):
        columns = [ColumnDef(**col) for col in t.get("columns", [])]
        tables.append(TableDef(name=t["name"], columns=columns, constraints=t.get("constraints", [])))  # name 필수, cons 선택

    return tables