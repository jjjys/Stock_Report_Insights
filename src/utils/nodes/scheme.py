from utils.nodes.database import DBNode

import yaml
from dataclasses import dataclass, field
from typing import List, Optional

import psycopg2


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