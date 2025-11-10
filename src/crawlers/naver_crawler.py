import os
import json
import time
import random
import logging
import requests
from urllib.parse import urljoin
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# 로깅 설정
logging.basicConfig(
    filename='crawler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class NaverPaySecuritiesCrawler:
    def __init__(self, max_pages=50, start_date=None, end_date=None):
        self.base_url = "https://finance.naver.com"
        self.categories = {
            "시황정보 리포트": "/research/market_info_list.naver",
            "투자정보 리포트": "/research/invest_list.naver",
            "종목분석 리포트": "/research/company_list.naver",
            "산업분석 리포트": "/research/industry_list.naver",
            "경제분석 리포트": "/research/economy_list.naver",
            "채권분석 리포트": "/research/debenture_list.naver"
        }
        # self.checkpoint_file = "crawler_checkpoint.json"  # 주석: 체크포인트 비활성화
        self.output_file = "naver_securities_reports.json"
        self.report_dir = "reports"  
        self.max_retries = 3
        self.wait_time = 5  # 대기 시간 증가
        self.max_pages = max_pages  # 수집 페이지 최대치. 필요에 따라 조정 가능

        # 날짜 설정 (URL 쿼리에만 사용, 체크포인트 저장 안 함)
        today = datetime.now().strftime('%Y-%m-%d')
        self.start_date = start_date or today
        self.end_date = end_date or today

        # 유효성 검사
        if self.start_date > self.end_date:
            raise ValueError(f"start_date({self.start_date})가 end_date({self.end_date})보다 클 수 없습니다.")

        # 체크포인트 비활성화: 항상 빈 데이터로 초기화
        self.data = {cat: {"data": []} for cat in self.categories}

    # def save_data(self):
    #     """수집된 데이터를 JSON 파일에 저장합니다."""
    #     try:
    #         final_data = {cat: self.data[cat]["data"] for cat in self.categories}
    #         with open(self.output_file, 'w', encoding='utf-8') as f:
    #             json.dump(final_data, f, ensure_ascii=False, indent=4)
    #         print(f"데이터가 {self.output_file}에 저장되었습니다.")
    #         logging.info(f"데이터가 {self.output_file}에 저장되었습니다.")
    #     except Exception as e:
    #         print(f"데이터 저장 중 에러: {e}")
    #         logging.error(f"데이터 저장 중 에러: {e}")

    def download_report(self, report_url, category, title, date, stock_name=""):  
        """Report 파일을 다운로드하여 저장합니다."""
        try:
            # 카테고리별 폴더 생성
            category_dir = os.path.join(self.report_dir, category.replace(" ", "_")) 
            os.makedirs(category_dir, exist_ok=True)
            
            # 파일명 생성 (특수문자 제거, 중복 방지: 날짜_[종목명]_제목.pdf)
            safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '_')).replace(" ", "_")
            safe_date = date.replace(".", "")
            safe_stock = "".join(c for c in stock_name if c.isalnum() or c in (' ', '_')).replace(" ", "_") if stock_name else ""
            report_filename = f"{safe_date}_[{safe_stock}]_{safe_title}.pdf" if safe_stock else f"{safe_date}_{safe_title}.pdf"

            report_path = os.path.join("data", category_dir, report_filename)
            os.makedirs(os.path.dirname(report_path), exist_ok=True)  # 상위 디렉토리 생성 보장
            
            # 이미 파일이 존재하면 스킵 (중복 방지)
            if os.path.exists(report_path):
                print(f"{report_filename} 이미 존재. 다운로드 스킵.")
                logging.info(f"{report_filename} 이미 존재. 다운로드 스킵.")
                return report_path
            
            # Report 다운로드
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/138.0.7204.97 Safari/537.36"
                )
            }
            response = requests.get(report_url, headers=headers, stream=True)
            if response.status_code == 200:
                with open(report_path, 'wb') as f:
                    f.write(response.content)
                logging.info(f"{report_filename} 다운로드 완료.")
                return report_path
            else:
                print(f"{report_filename} 다운로드 실패: HTTP {response.status_code}")
                logging.error(f"{report_filename} 다운로드 실패: HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"Report 다운로드 중 에러: {e}")
            logging.error(f"Report 다운로드 중 에러: {e}")
            return None
        
    def navigate_to_page(self, driver, url, page_num):
        """특정 페이지로 이동합니다. (날짜 필터링 쿼리 추가)"""
        for attempt in range(self.max_retries):
            try:
                # 날짜 필터링 쿼리 추가
                query_params = f"?keyword=&searchType=writeDate&writeFromDate={self.start_date}&writeToDate={self.end_date}&page={page_num}"
                full_url = f"{self.base_url}{url}{query_params}"
                print(f"{full_url}로 이동 중 (시도 {attempt + 1}/{self.max_retries})")
                logging.info(f"{full_url}로 이동 중")
                driver.get(full_url)
                WebDriverWait(driver, self.wait_time).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#contentarea_left > div.box_type_m > table.type_1"))
                )
                # 랜덤 스크롤로 봇 탐지 회피
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(0.5, 2))
                print(f"페이지 {page_num}로 이동 성공")
                return True
            except Exception as e:
                print(f"페이지 {page_num} 이동 중 에러: {e}")
                logging.error(f"페이지 {page_num} 이동 중 에러: {e}")
                if attempt == self.max_retries - 1:
                    return False
                time.sleep(random.uniform(1, 3))
        return False

    def extract_table_data(self, driver, category):
        """현재 페이지에서 테이블 데이터를 추출합니다."""
        try:
            print(f"{category} 카테고리 데이터 추출 중")
            logging.info(f"{category} 카테고리 데이터 추출 중")
            table = driver.find_element(By.CSS_SELECTOR, "#contentarea_left > div.box_type_m > table.type_1")
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # 헤더 행 제외
            data_list = []

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 4:  # 열 개수 확인
                    continue

                # Report 링크 추출
                report_link = None
                report_col_index = 3 if category in ["종목분석 리포트", "산업분석 리포트"] else 2
                try:
                    report_anchor = cols[report_col_index].find_element(By.TAG_NAME, "a")
                    report_href = report_anchor.get_attribute("href")
                    if report_href and report_href.endswith(".pdf"):
                        report_link = urljoin(self.base_url, report_href)
                except:
                    report_link = None
                
                # 데이터 구성 (키 변경)
                if category == "종목분석 리포트":
                    row_data = {
                        "종목명": cols[0].text.strip(),
                        "제목": cols[1].text.strip(),
                        "증권사": cols[2].text.strip(),
                        "Report_url": report_link,
                        "작성일": cols[4].text.strip() if len(cols) > 4 else "",
                        "조회수": cols[5].text.strip() if len(cols) > 5 else ""
                    }
                elif category == "산업분석 리포트":
                    row_data = {
                        "분류": cols[0].text.strip(),
                        "제목": cols[1].text.strip(),
                        "증권사": cols[2].text.strip(),
                        "Report_url": report_link,  
                        "작성일": cols[4].text.strip() if len(cols) > 4 else "",
                        "조회수": cols[5].text.strip() if len(cols) > 5 else ""
                    }
                else:
                    row_data = {
                        "제목": cols[0].text.strip(),
                        "증권사": cols[1].text.strip(),
                        "Report_url": report_link,  
                        "작성일": cols[3].text.strip() if len(cols) > 3 else "",
                        "조회수": cols[4].text.strip() if len(cols) > 4 else ""
                    }

                if report_link:
                    stock_name = row_data.get("종목명", "")  # 종목명 추출 (없으면 빈 문자열)
                    report_path = self.download_report(report_link, category, row_data["제목"], row_data["작성일"], stock_name)
                    row_data["Report_local_path"] = report_path if report_path else None 
                    date = row_data["작성일"].replace(".", "")
                    stock = ''.join(c for c in row_data["종목명"] if c.isalnum() or c in (" ", "_")).replace(" ", "_")
                    title = ''.join(c for c in row_data["제목"] if c.isalnum() or c in (" ", "_")).replace(" ", "_")
                    row_data["report_name"] = f"{date}_[{stock}]_{title}.pdf"
                
                # 중복 체크: 매번 빈 self.data이니 항상 추가 (파일 체크로 보완)
                if row_data not in self.data[category]["data"]:
                    data_list.append(row_data)
            return data_list
        except Exception as e:
            print(f"{category} 테이블 데이터 추출 중 에러: {e}")
            logging.error(f"{category} 테이블 데이터 추출 중 에러: {e}")
            return []
        
    def parse_max_page_from_pagination(self, driver):
            """하단 페이지네이션에서 [맨뒤] 링크의 page 값을 추출해 최대 페이지 반환.
            실패 시 self.max_pages 반환.
            """
            try:
                # WebDriverWait로 "맨뒤" 링크 대기 (동적 로딩 대응)
                last_link = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".pgRR a"))
                )
                href = last_link.get_attribute("href")
                if not href:
                    raise ValueError("href 속성 없음")
                
                # URL 쿼리 파싱 (e.g., /research/company_list.naver?...&page=3 → {'page': ['3']})
                parsed_url = urlparse(href)
                query_params = parse_qs(parsed_url.query)
                page_str = query_params.get('page', [None])[0]  # page= 값 추출 (리스트 첫 번째)
                
                if page_str:
                    max_page = int(page_str)
                    print(f"최대 페이지 파싱 성공: {max_page}")
                    logging.info(f"최대 페이지 파싱: {max_page}")
                    return max_page
                else:
                    raise ValueError("page 쿼리 파라미터 없음")
            except Exception as e:
                print(f"최대 페이지 파싱 실패 ({e}). 기본 {self.max_pages}페이지로 fallback.")
                logging.warning(f"최대 페이지 파싱 실패: {e}. Fallback to {self.max_pages}")
                return self.max_pages  # 실패 시 기본 최대 페이지 반환
            
    def crawl_category(self, driver, category, url):
        """주어진 카테고리의 모든 페이지를 크롤링합니다. (매번 페이지 1부터 시작)"""
        try:
            page_num = 1
            print(f"{category} 크롤링 시작 (페이지 {page_num}부터, 기간: {self.start_date} ~ {self.end_date})")
            logging.info(f"{category} 크롤링 시작")

            # 첫 페이지로 이동
            if not self.navigate_to_page(driver, url, page_num):
                print(f"{category} 첫 페이지 이동 실패.")
                return

            # 최대 페이지 파싱 (첫 페이지에서 수행)
            self.max_pages = self.parse_max_page_from_pagination(driver)
            print(f"{category} 최대 페이지: {self.max_pages}")
            
            while page_num <= self.max_pages:
                if not self.navigate_to_page(driver, url, page_num):
                    print(f"{category}의 페이지 {page_num} 이동 실패. 중단합니다.")
                    logging.warning(f"{category}의 페이지 {page_num} 이동 실패")
                    break

                page_data = self.extract_table_data(driver, category)
                if not page_data:
                    print(f"{category}의 페이지 {page_num}에서 데이터 없음. 중단합니다.")
                    logging.info(f"{category}의 페이지 {page_num}에서 데이터 없음")
                    break

                self.data[category]["data"].extend(page_data)  # 빈 self.data이니 전체 추가
                # self.save_checkpoint()  # 주석: 체크포인트 저장 비활성화
                print(f"{category}의 페이지 {page_num}에서 {len(page_data)}개 레코드 수집")
                logging.info(f"{category}의 페이지 {page_num}에서 {len(page_data)}개 레코드 수집")
                time.sleep(random.uniform(1, 3))

                page_num += 1
        except Exception as e:
            print(f"{category} 크롤링 중 에러: {e}")
            logging.error(f"{category} 크롤링 중 에러: {e}")
            # self.save_checkpoint()  # 주석: 에러 시 저장 비활성화

    def run(self, driver=None):  # 변경: driver 파라미터 옵션으로 (외부에서 전달 가능)
        """크롤러를 실행하는 메인 메서드입니다."""
        options = uc.ChromeOptions()
        options.add_argument('--headless')  # headless 모드
        options.add_argument('--incognito')  # 시크릿 모드
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')

        if driver is None:
            driver = uc.Chrome(options=options, version_main=141)  # 자동 버전 감지
        try:
            # 사용자 에이전트 설정 (uc에서 기본적으로 랜덤 UA 사용, 필요시 오버라이드)
            driver.execute_cdp_cmd(
                "Network.setUserAgentOverride",
                {
                    "userAgent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/138.0.7204.97 Safari/537.36"
                    )
                }
            )
            driver.set_window_size(1920, 1080)
            print("브라우저 설정 완료.")
            logging.info("브라우저 설정 완료.")
            # 크롤링 메인 작업
            for category, url in self.categories.items():
                # 특정 카테고리 스킵
                if category == '시황정보 리포트'\
                or category == '투자정보 리포트'\
                or category == '산업분석 리포트'\
                or category == '경제분석 리포트'\
                or category == '채권분석 리포트':
                    continue
                print(f"{category} 처리 중 (기간: {self.start_date} ~ {self.end_date})")
                logging.info(f"{category} 처리 중")
                self.crawl_category(driver, category, url) 
            # self.save_data()
            print("네이버 제공 증권사 레포트 웹크롤링 작업 완료.")
            logging.info("네이버 제공 증권사 레포트 웹크롤링 작업 완료.")
        except Exception as e:
            print(f"메인 실행 중 에러: {e}")
            logging.error(f"메인 실행 중 에러: {e}")
            # self.save_checkpoint()  # 주석: 에러 시 저장 비활성화
        finally:
            if driver is not None:
                driver.quit()  # 드라이버 종료 추가

if __name__ == "__main__":
    # 예시: 기본 오늘 날짜 사용 (매번 처음부터 수집)
    #crawler = NaverPaySecuritiesCrawler()
    #crawler = NaverPaySecuritiesCrawler(start_date='2025-10-02', end_date='2025-10-02')
    #crawler = NaverPaySecuritiesCrawler(start_date='2021-02-16', end_date='2021-02-18')
    crawler = NaverPaySecuritiesCrawler(start_date='2025-11-07', end_date='2025-11-07')
    crawler.run()
    print(f"데이터 확인=========================")
    print(f"종목명:{crawler.data['종목분석 리포트']['data'][0]['종목명']}")
    print(f"제목:{crawler.data['종목분석 리포트']['data'][0]['제목']}")
    print(f"증권사:{crawler.data['종목분석 리포트']['data'][0]['증권사']}")
    print(f"Report_url:{crawler.data['종목분석 리포트']['data'][0]['Report_url']}")
    print(f"작성일:{crawler.data['종목분석 리포트']['data'][0]['작성일']}")
    print(f"조회수:{crawler.data['종목분석 리포트']['data'][0]['조회수']}")
    print(f"report_name:{crawler.data['종목분석 리포트']['data'][0]['report_name']}")
    print(f"Report_local_path:{crawler.data['종목분석 리포트']['data'][0]['Report_local_path']}")


'''
1. 수집 확인
len(crawler.data['종목분석 리포트']['data']) != 0

2. DB 적재

-- reports 테이블 (raw 데이터)
CREATE TABLE reports (
    id SERIAL PRIMARY KEY,
    post_date DATE NOT NULL, -- 레포트 포스트된 날짜(작성일)
    report_name VARCHAR(255) NOT NULL, -- (제목)
    report_url TEXT UNIQUE,  -- (Report_url)
    report_preprocessed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW() -- db 적재 날짜
);
'''