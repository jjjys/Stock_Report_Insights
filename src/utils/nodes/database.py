from utils.nodes.cores import Node

import psycopg2
import os


class DBNode(Node):
    """
    DB 연결 노드는 DBNode 상속하여 생성자 통해 conn, cursor 객체 자동 생성.
    생성자 오버라이드 시 반드시 conn, cursor 객체 생성 필요.
    호출 함수(__call__)는 오버라이드 권장.
    """
    def __init__(self, conn=None, cursor=None):
        if conn is not None:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('POSTGRES_KEY'),
                                         host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'))
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
            print("[DBNode] DB Request Success !!")

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

        if conn is not None:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('POSTGRES_KEY'),
                                         host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'))
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
            print("DB INSERT Success !!")

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

        if conn is not None:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('POSTGRES_KEY'),
                                         host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'))
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

        return self.cursor.fetchall()


class DBClose(DBNode):
    """
    DB 작업 종료 후 반드시 사용 (MultiThreadNode 혹은 MapNode 내에서 사용하지 않음 권장)

    Args:
        last_commit (bool): 마지막 커밋 여부 (safe close)
        toss_input (bool): 입력 데이터 출력 여부 (후속 노드 데이터 전달)
    """
    def __init__(self, last_commit:bool=True, toss_input:bool=True):
        self.last_commit = last_commit
        self.toss_input = toss_input

    def __call__(self, result, *args, **kwargs):
        if self.last_commit:
            self.conn.commit()

        self.conn.close()

        if self.toss_input:
            return result