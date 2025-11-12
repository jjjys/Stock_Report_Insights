from utils.nodes.cores import Node

from utils.logger import log_function
import logging

from multipledispatch import dispatch
from datetime import datetime, timedelta

from pykrx import stock


# ------------------------------
# 각 노드 정의 및 구현
# ------------------------------
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

    @dispatch(dict) # (extracted_data_dict)
    def __call__(self, trt:dict) -> dict:
        # 파이프라인 내 노드로 사용
        trt = {"ticker": trt.get(self.ticker_key, None),
               "report_date": trt.get(self.report_date_key, None),
               "target_price": trt.get(self.target_price_key, None),
               "report_id": trt.get("report_id", None),
               "llm_id": trt.get("llm_id", None)}

        return self.krx_target_hitter(**trt)

    @dispatch(tuple)  # (extracted_data_dict, (report_id, llm_id))
    def __call__(self, trt_i:tuple[dict, dict]) -> dict:
        # 파이프라인 내 노드로 사용
        trt = {"ticker": trt_i[0].get(self.ticker_key, None),
               "report_date": trt_i[0].get(self.report_date_key, None),
               "target_price": trt_i[0].get(self.target_price_key, None),
               "report_id": trt_i[1].get("report_id", None),  # 명칭 스키마 단 고정
               "llm_id": trt_i[1].get("llm_id", None)}

        return self.krx_target_hitter(**trt)

    @dispatch(str, str, int, int, int)
    def __call__(self, ticker:str, report_date:str, target_price:int, report_id:int, llm_id:int) -> dict:
        # 직접 호출 시 사용
        return self.krx_target_hitter(ticker, report_date, target_price, report_id, llm_id)

    @log_function(logging.INFO)
    def krx_target_hitter(self, ticker:str, report_date:str, target_price:int, report_id:int=None, llm_id:int=None) -> dict:
        start_date = report_date.replace("-", "")
        end_date = datetime.today().strftime("%Y%m%d")

        print("[KrxTargetHitter] KRX 데이터 호출 중...")
        df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
        df = df[["종가"]]

        reached = df[df["종가"] >= target_price]

        if not reached.empty:
            first_hit_date = reached.index[0].strftime("%Y-%m-%d")

            report_dt = datetime.strptime(report_date, "%Y-%m-%d")
            hit_dt = datetime.strptime(first_hit_date, "%Y-%m-%d")

            print("[KrxTargetHitter] Target Hit !!")
            return {"report_id": report_id, "llm_id": llm_id, "target_price_reached_date": first_hit_date, "days_to_reach": (hit_dt - report_dt).days}
        else:
            raise ValueError("[KrxTargetHitter] 목표 주가 미도달")