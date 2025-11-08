from cores.cores import DBNode

import psycopg2


class KrxDB(DBNode):
    def __call__(self, values:dict): # (report_id, llm_id, target_price_reached_date, days_to_reach)
        report_id = values["report_id"]
        llm_id = values["llm_id"]
        target_price_reached_date = values["target_price_reached_date"]
        days_to_reach = values["days_to_reach"]

        try:
            self.cursor.execute("""
                INSERT INTO krx (id, llm_id, target_price_reached_date, days_to_reach)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
            """, (report_id, llm_id, target_price_reached_date, days_to_reach))
        except (Exception, psycopg2.Error) as e:
            print(f"[ReportExtractionsDB] INSERT Error: table (KRX) {(report_id, llm_id, target_price_reached_date, days_to_reach)}\n{e}")
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
        finally:
            if self.inner_conn:
                self.conn.close()