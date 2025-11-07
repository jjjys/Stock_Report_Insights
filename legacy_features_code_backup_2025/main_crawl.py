import os
import json
import time
import random
import logging
import requests
from urllib.parse import urljoin
from seleniumbase import SB
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 로깅 설정
logging.basicConfig(
    filename='crawler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class NaverPaySecuritiesCrawler:
    def __init__(self):
        self.base_url = "https://finance.naver.com"
        self.categories = {
            "시황정보 리포트": "/research/market_info_list.naver",
            "투자정보 리포트": "/research/invest_list.naver",
            "종목분석 리포트": "/research/company_list.naver",
            "산업분석 리포트": "/research/industry_list.naver",
            "경제분석 리포트": "/research/economy_list.naver",
            "채권분석 리포트": "/research/debenture_list.naver"
        }
        self.checkpoint_file = "crawler_checkpoint.json"
        self.output_file = "naver_securities_reports.json"
        self.pdf_dir = "pdfs"
        self.data = self.load_checkpoint()
        self.max_retries = 3
        self.wait_time = 5  # 대기 시간 증가
        self.max_pages = 2000  # 필요에 따라 조정 가능

    def load_checkpoint(self):
        """체크포인트 데이터를 로드합니다."""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    print("체크포인트 파일에서 데이터 로드 중...")
                    logging.info("체크포인트 파일에서 데이터 로드 완료.")
                    return json.load(f)
            return {cat: {"data": [], "last_page": 1} for cat in self.categories}
        except Exception as e:
            print(f"체크포인트 로드 중 에러: {e}")
            logging.error(f"체크포인트 로드 중 에러: {e}")
            return {cat: {"data": [], "last_page": 1} for cat in self.categories}

    def save_checkpoint(self):
        """현재 진행 상황을 체크포인트 파일에 저장합니다."""
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)
            print("체크포인트 저장 완료.")
            logging.info("체크포인트 저장 완료.")
        except Exception as e:
            print(f"체크포인트 저장 중 에러: {e}")
            logging.error(f"체크포인트 저장 중 에러: {e}")

    def save_data(self):
        """수집된 데이터를 JSON 파일에 저장합니다."""
        try:
            final_data = {cat: self.data[cat]["data"] for cat in self.categories}
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=4)
            print(f"데이터가 {self.output_file}에 저장되었습니다.")
            logging.info(f"데이터가 {self.output_file}에 저장되었습니다.")
        except Exception as e:
            print(f"데이터 저장 중 에러: {e}")
            logging.error(f"데이터 저장 중 에러: {e}")

    def download_pdf(self, pdf_url, category, title, date):
        """PDF 파일을 다운로드하여 저장합니다."""
        try:
            # 카테고리별 폴더 생성
            category_dir = os.path.join(self.pdf_dir, category.replace(" ", "_"))
            os.makedirs(category_dir, exist_ok=True)
            
            # 파일명 생성 (특수문자 제거)
            safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '_')).replace(" ", "_")
            safe_date = date.replace(".", "")
            pdf_filename = f"{safe_date}_{safe_title}.pdf"  # 수정: 날짜_제목 형식으로 변경
            pdf_path = os.path.join(category_dir, pdf_filename)
            
            # 이미 파일이 존재하면 스킵
            if os.path.exists(pdf_path):
                print(f"{pdf_filename} 이미 존재. 다운로드 스킵.")
                logging.info(f"{pdf_filename} 이미 존재. 다운로드 스킵.")
                return pdf_path
            
            # PDF 다운로드
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/138.0.7204.97 Safari/537.36"
                )
            }
            response = requests.get(pdf_url, headers=headers, stream=True)
            if response.status_code == 200:
                with open(pdf_path, 'wb') as f:
                    f.write(response.content)
                #print(f"{pdf_filename} 다운로드 완료.")
                logging.info(f"{pdf_filename} 다운로드 완료.")
                return pdf_path
            else:
                print(f"{pdf_filename} 다운로드 실패: HTTP {response.status_code}")
                logging.error(f"{pdf_filename} 다운로드 실패: HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"{pdf_filename} 다운로드 중 에러: {e}")
            logging.error(f"{pdf_filename} 다운로드 중 에러: {e}")
            return None
        
    def navigate_to_page(self, sb, url, page_num):
        """특정 페이지로 이동합니다."""
        for attempt in range(self.max_retries):
            try:
                full_url = f"{self.base_url}{url}?page={page_num}"
                print(f"{full_url}로 이동 중 (시도 {attempt + 1}/{self.max_retries})")
                logging.info(f"{full_url}로 이동 중")
                sb.open(full_url)
                WebDriverWait(sb.driver, self.wait_time).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#contentarea_left > div.box_type_m > table.type_1"))
                )
                # 랜덤 스크롤로 봇 탐지 회피
                sb.execute_script("window.scrollTo(0, document.body.scrollHeight);")
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

    def extract_table_data(self, sb, category):
        """현재 페이지에서 테이블 데이터를 추출합니다."""
        try:
            print(f"{category} 카테고리 데이터 추출 중")
            logging.info(f"{category} 카테고리 데이터 추출 중")
            table = sb.find_element(By.CSS_SELECTOR, "#contentarea_left > div.box_type_m > table.type_1")
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # 헤더 행 제외
            data_list = []

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 4:  # 열 개수 확인
                    continue

                # PDF 링크 추출
                pdf_link = None
                pdf_col_index = 3 if category in ["종목분석 리포트", "산업분석 리포트"] else 2
                try:
                    pdf_anchor = cols[pdf_col_index].find_element(By.TAG_NAME, "a")
                    pdf_href = pdf_anchor.get_attribute("href")
                    if pdf_href and pdf_href.endswith(".pdf"):
                        pdf_link = urljoin(self.base_url, pdf_href)
                except:
                    pdf_link = None
                
                # 데이터 구성
                if category == "종목분석 리포트":
                    row_data = {
                        "종목명": cols[0].text.strip(),
                        "제목": cols[1].text.strip(),
                        "증권사": cols[2].text.strip(),
                        "PDF": pdf_link,
                        "작성일": cols[4].text.strip() if len(cols) > 4 else "",
                        "조회수": cols[5].text.strip() if len(cols) > 5 else ""
                    }
                elif category == "산업분석 리포트":
                    row_data = {
                        "분류": cols[0].text.strip(),
                        "제목": cols[1].text.strip(),
                        "증권사": cols[2].text.strip(),
                        "PDF": pdf_link,
                        "작성일": cols[4].text.strip() if len(cols) > 4 else "",
                        "조회수": cols[5].text.strip() if len(cols) > 5 else ""
                    }
                else:
                    row_data = {
                        "제목": cols[0].text.strip(),
                        "증권사": cols[1].text.strip(),
                        "PDF": pdf_link,
                        "작성일": cols[3].text.strip() if len(cols) > 3 else "",
                        "조회수": cols[4].text.strip() if len(cols) > 4 else ""
                    }

                # PDF 다운로드
                if pdf_link:
                    pdf_path = self.download_pdf(pdf_link, category, row_data["제목"], row_data["작성일"])
                    row_data["PDF_local_path"] = pdf_path if pdf_path else None
                
                if row_data not in self.data[category]["data"]:
                    data_list.append(row_data)
            return data_list
        except Exception as e:
            print(f"{category} 테이블 데이터 추출 중 에러: {e}")
            logging.error(f"{category} 테이블 데이터 추출 중 에러: {e}")
            return []

    def crawl_category(self, sb, category, url):
        """주어진 카테고리의 모든 페이지를 크롤링합니다."""
        try:
            last_page = self.data[category]["last_page"]
            print(f"{category} 크롤링 시작 (페이지 {last_page}부터)")
            logging.info(f"{category} 크롤링 시작 (페이지 {last_page}부터)")

            while last_page <= self.max_pages:
                if not self.navigate_to_page(sb, url, last_page):
                    print(f"{category}의 페이지 {last_page} 이동 실패. 중단합니다.")
                    logging.warning(f"{category}의 페이지 {last_page} 이동 실패")
                    break

                page_data = self.extract_table_data(sb, category)
                if not page_data:
                    print(f"{category}의 페이지 {last_page}에서 데이터 없음. 중단합니다.")
                    logging.info(f"{category}의 페이지 {last_page}에서 데이터 없음")
                    break

                self.data[category]["data"].extend(page_data)
                self.data[category]["last_page"] = last_page + 1
                self.save_checkpoint()
                print(f"{category}의 페이지 {last_page}에서 {len(page_data)}개 레코드 수집")
                logging.info(f"{category}의 페이지 {last_page}에서 {len(page_data)}개 레코드 수집")
                time.sleep(random.uniform(1, 3))

                # # 동적 페이지네이션 확인
                # next_button = sb.find_elements("a.next")
                # if not next_button or "disabled" in next_button[0].get_attribute("class"):
                #     print(f"{category}의 마지막 페이지 도달. 중단합니다.")
                #     logging.info(f"{category}의 마지막 페이지 도달")
                #     break
                
                end_page = 999
                if last_page == end_page:
                    print(f"{category}의 종료 페이지({end_page}) 도달. 중단합니다.")
                    logging.info(f"{category}의 종료 페이지({end_page}) 도달")
                    break

                last_page += 1
        except Exception as e:
            print(f"{category} 크롤링 중 에러: {e}")
            logging.error(f"{category} 크롤링 중 에러: {e}")
            self.save_checkpoint()

    def run(self):
        """크롤러를 실행하는 메인 메서드입니다."""
        with SB(
            headless=True,
            undetectable=True,
            incognito=True
        ) as sb:
            try:
                # 사용자 에이전트 설정
                sb.driver.execute_cdp_cmd(
                    "Network.setUserAgentOverride",
                    {
                        "userAgent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/138.0.7204.97 Safari/537.36"
                        )
                    }
                )
                sb.set_window_size(1920, 1080)
                print("브라우저 설정 완료.")
                logging.info("브라우저 설정 완료.")
                # 크롤링 메인 작업
                for category, url in self.categories.items():
                    if category == '경제분석 리포트' or category == '채권분석 리포트':
                        continue
                    print(f"{category} 처리 중")
                    logging.info(f"{category} 처리 중")
                    self.crawl_category(sb, category, url)
                self.save_data()
                print("크롤링 성공적으로 완료.")
                logging.info("크롤링 성공적으로 완료.")
            except Exception as e:
                print(f"메인 실행 중 에러: {e}")
                logging.error(f"메인 실행 중 에러: {e}")
                self.save_checkpoint()

if __name__ == "__main__":
    crawler = NaverPaySecuritiesCrawler()
    crawler.run()