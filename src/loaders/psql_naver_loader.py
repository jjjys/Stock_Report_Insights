from utils.nodes.cores import Node
from utils.nodes.database import DBNode
from datetime import datetime
from utils.logger import log_function
import logging

import psycopg2


class Crawler(Node):
    def __call__(self, crawler:list):
        return crawler


class ReportDB(DBNode):
    @log_function(logging.INFO)
    def __call__(self, values:dict):
        # report_name 키 체크 (없으면 제목으로 대체)
        report_name = values.get("report_name") or values.get("제목", "Unknown")
        report_url = values.get("Report_url")
        post_date = values.get("작성일")

        # 필수 필드 체크
        if not report_name or not post_date:
            print(f"[ReportDB] 필수 필드 부족: report_name={report_name}, post_date={post_date}")
            return

        # 날짜 형식 변환
        try:
            post_date = datetime.strptime(post_date, "%y.%m.%d").date()  # '23.02.16' -> '2023-02-16'
        except ValueError as e:
            print(f"[ReportDB] Date format error: {e}")
            return  # 또는 적절한 예외 처리
        # 디버그: 연결 정보와 입력값 출력
        try:
            conn_info = getattr(self.conn, 'get_dsn_parameters', None)
            dsn = conn_info() if callable(conn_info) else getattr(self.conn, 'dsn', str(self.conn))
        except Exception:
            dsn = "UNKNOWN_CONN"
        print(f"[ReportDB] DB DSN: {dsn}")
        print(f"[ReportDB] INSERT VALUES: post_date={post_date}, report_name={report_name}, report_url={report_url}")


        try:
            self.cursor.execute("""
                INSERT INTO reports (post_date, report_name, report_url)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;
            """, (post_date, report_name, report_url))
        except (Exception, psycopg2.Error) as e:
            print(f"[ReportDB] INSERT Error: table (REPORTS) {(post_date, report_name, report_url)}\n{e}")
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            # self.cursor.execute("""
            #     SELECT id FROM reports WHERE report_name = %s;
            # """, report_name)
            # report_id = self.cursor.fetchone()[0]