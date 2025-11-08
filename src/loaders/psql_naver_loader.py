from src.cores.cores import Node, DBNode

import psycopg2

class ReportDB(DBNode):
    def __call__(self, values:dict):
        report_name = values["report_name"]
        report_url = values["report_url"]
        post_date = values["post_date"]

        try:
            self.cursor.execute(f"""
                INSERT INTO reports (post_date, report_name, report_url)
                VALUES (%s, %s, %s);
            """, (post_date, report_name, report_url))
        except (Exception, psycopg2.Error) as e:
            print(f"[ReportDB] INSERT Error: table (REPORTS) {(post_date, report_name, report_url)}\n{e}")
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            # self.cursor.execute(f"""
            #     SELECT id FROM reports WHERE report_name = %s;
            # """, report_name)
            # report_id = self.cursor.fetchone()[0]
        finally:
            if self.inner_conn:
                self.conn.close()
            # return report_id # 추가 반환 데이터 필요