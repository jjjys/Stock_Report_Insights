from cores.cores import DBNode

import psycopg2
import os, shutil
from dotenv import load_dotenv

load_dotenv()


class ReportExtractionsDB(DBNode):
    def __call__(self, values:dict) -> dict: # (report_name, llm_type, llm_version, stock, ticker, investment_opinion, published_date, current_price, target_price, author, firm)
        report_name = values["report_name"]
        llm_type = values["llm_type"]
        llm_version = values["llm_version"]
        stock = values["stock"]
        ticker = values["ticker"]
        investment_opinion = values["investment_opinion"]
        published_date = values["published_date"]
        current_price = values["current_price"]
        target_price = values["target_price"]
        author = values["author"]
        firm = values["firm"]
        report_id = None
        llm_id = None
        analyst_id = None

        try:
            self.cursor.execute(f"""
                INSERT INTO stock_info (stock, ticker)
                VALUES (%s, %s) ON CONFLICT DO NOTHING;
                INSERT INTO llm (type, version)
                VALUES (%s, %s) ON CONFLICT DO NOTHING;
                INSERT INTO analyst (name, firm)
                VALUES (%s, %s) ON CONFLICT DO NOTHING;
            """, (stock, ticker, llm_type, llm_version, author, firm))
        except (Exception, psycopg2.Error) as e:
            self.conn.rollback()
            print(f"[ReportExtractionsDB] INSERT Error: \ntable (STOCK_INFO) {(stock, ticker)} // (LLM) {(llm_type, llm_version)} // (ANALYST) {(author, firm)})\n{e}")
            # raise
        else:
            self.conn.commit()
            self.cursor.execute(f"""
                SELECT id FROM reports WHERE report_name = %s;
            """, (report_name,))
            report_id = self.cursor.fetchone()[0]
            # print(report_name, "==> report_id:", report_id)
            self.cursor.execute(f"""
                SELECT stock_id FROM stock_info WHERE ticker = %s;
            """, (ticker,))
            stock_id = self.cursor.fetchone()[0]
            # print("ticker=", ticker, "==> stock_id:", stock_id)
            self.cursor.execute(f"""
                SELECT llm_id FROM llm WHERE type = %s AND version = %s;
            """, (llm_type, llm_version))
            llm_id = self.cursor.fetchone()[0]
            # print("llm_id:", llm_id)
            self.cursor.execute("""
                SELECT analyst_id FROM analyst WHERE name = %s AND firm = %s;
            """, (author, firm))
            analyst_id = self.cursor.fetchone()[0]
            # print("analyst_id:", analyst_id)

            try:
                self.cursor.execute("""
                    INSERT INTO report_extractions (id, llm_id, investment_opinion, stock_id, published_date, current_price, target_price, analyst_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
                """, (report_id, llm_id, investment_opinion.lower(), stock_id, published_date, current_price, target_price, analyst_id))
            except (Exception, psycopg2.Error) as e:
                print(f"[ReportExtractionsDB] INSERT Error: \ntable (REPORT_EXTRACTIONS) \
                      {(report_id, llm_id, investment_opinion.lower(), stock_id, published_date, current_price, target_price, analyst_id)}\n{e}")
            else:
                try:
                    self.cursor.execute("""
                        UPDATE reports SET report_preprocessed = TRUE WHERE id = %s;
                    """, (report_id,))
                except (Exception, psycopg2.Error) as e:
                    self.conn.rollback()
                    print(f"[ReportExtractionsDB] UPDATE Error: table (REPORTS) {report_id}\n{e}")
                    # raise
                else:
                    self.conn.commit()  # reports에 report_extractions 적재 사실 업데이트 실패 시 적재 내용도 롤백
                    shutil.move(os.path.join(os.getenv('REPORTS_PATH'), report_name), os.path.join(os.getenv('REPORTS_FINISHED_PATH'))) # 정보 추출 파일 이동
                finally:
                    # self.conn.close()
                    return {"report_id": report_id, "llm_id": llm_id}