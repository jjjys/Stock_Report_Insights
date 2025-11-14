from extractors.llm_extractor import DocumentsLoader, LLMFeatsExtractor
from extractors.prompt import system_prompt as prompt
from crawlers.krx_extractor import KrxTargetHitter
from loaders.psql_naver_loader import Crawler, ReportDB
from loaders.psql_llm_loader import ReportExtractionsDB
from loaders.psql_krx_loader import KrxDB, KrxHitDump

from utils.logger import setup_logger
from dotenv import load_dotenv
import psycopg2
import os
from crawlers.naver_crawler import NaverPaySecuritiesCrawler

setup_logger()
load_dotenv()


def load_raw_reports(start_date:str=None, end_date:str=None, conn=None, cursor=None):
    crawler_node = Crawler()
    report_insert = ReportDB(conn, cursor)

    crawler = NaverPaySecuritiesCrawler(start_date=start_date, end_date=end_date)
    crawler.run()

    (crawler_node // report_insert)(crawler.data['종목분석 리포트']['data'])


def route_llm_extractor():
    pass # 적절한 LLM 추출기 라우팅


def load_report_feats(llm_type:str, llm_version:str, api_key:str=None, conn=None, cursor=None):
    reports_getter = DocumentsLoader(os.getenv('REPORTS_PATH'))
    extractor = LLMFeatsExtractor(os.getenv('REPORTS_PATH'), llm_type, llm_version, prompt, api_key=api_key, 
                                  essential_cols=("ticker", "published_date", "target_price"))
    report_ex_insert = ReportExtractionsDB(conn, cursor)

    (reports_getter // (extractor - report_ex_insert))(None)


def load_krx_hit(conn=None, cursor=None):
    krx_dump = KrxHitDump("ticker", "published_date", "target_price")
    krx_hitter = KrxTargetHitter("ticker", "published_date", "target_price")
    krx_insert = KrxDB(conn, cursor)
    
    (krx_dump // (krx_hitter - krx_insert))(None)


if __name__ == "__main__":
    # pass
    with psycopg2.connect(dbname=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('POSTGRES_KEY'), host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT')) as conn:
        with conn.cursor() as cursor:
            load_raw_reports(conn=conn, cursor=cursor)
            route_llm_extractor()
            load_report_feats("gemini", "2.5-flash", os.getenv('GEMINI_API_KEY'), conn, cursor)
            load_krx_hit(conn, cursor)