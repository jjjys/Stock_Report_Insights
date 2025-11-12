from utils.nodes.database import DBNode

from utils.logger import log_function
import logging

import psycopg2
import os


class KrxDB(DBNode):
    @log_function(logging.INFO)
    def __call__(self, values:dict): # (report_id, llm_id, target_price_reached_date, days_to_reach)
        report_id = values["report_id"]
        llm_id = values["llm_id"]
        target_price_reached_date = values["target_price_reached_date"]
        days_to_reach = values["days_to_reach"]

        try:
            self.cursor.execute("""
                INSERT INTO krx (id, llm_id, target_price_reached_date, days_to_reach)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
                UPDATE report_extractions SET krx_loaded = TRUE WHERE id = %s AND llm_id = %s;
            """, (report_id, llm_id, target_price_reached_date, days_to_reach, report_id, llm_id))
        except (Exception, psycopg2.Error) as e:
            print(f"[ReportExtractionsDB] INSERT Error: table (KRX) {(report_id, llm_id, target_price_reached_date, days_to_reach)}\n{e}")
            self.conn.rollback()
            raise
        else:
            self.conn.commit()


class KrxHitDump(DBNode):
    def __init__(self, ticker_key:str=None, report_date_key:str=None, target_price_key:str=None, conn=None, cursor=None):
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

        if conn is not None:
            self.conn = conn
            self.cursor = cursor
        else:
            self.conn = psycopg2.connect(dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('POSTGRES_KEY'),
                                         host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'))
            self.cursor = self.conn.cursor()
    
    @log_function(logging.INFO)
    def __call__(self, *arg, **kwargs):
        self.cursor.execute("""
            SELECT r.id, r.llm_id, s.ticker, TO_CHAR(r.published_date, 'YYYY-MM-DD'), r.target_price
            FROM report_extractions r
            LEFT JOIN stock_info s ON r.stock_id = s.stock_id 
            WHERE r.krx_loaded = False;
        """)

        cols = ("report_id", "llm_id", self.ticker_key, self.report_date_key, self.target_price_key)
        
        # return self.cursor.fetchall()
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]