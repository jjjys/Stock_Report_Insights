from dotenv import load_dotenv

from google import genai

import numpy as np
import pandas as pd

from datetime import datetime
import os, subprocess, time, json, re, shutil
import warnings
from typing import Callable

from concurrent.futures import ThreadPoolExecutor, as_completed#, ProcessPoolExecutor

from pykrx import stock
import psycopg2

IS_COLAB_ENV = False# is_colab()

pdf_path = 'D:/jjjys/pdfs'
pdf_finished_path = 'D:/jjjys/pdf_finished'

def get_report_pdf_files(directory_path, is_test:bool=False, test_num:int=5, verbose:bool=False) -> list:
    all_items = os.listdir(directory_path)

    # PDF 필터링
    pdf_files = [item for item in all_items if item.endswith('.pdf')]

    if is_test:
        pdf_files = pdf_files[:test_num]

    if verbose:
      print("Selected PDF files for processing:")
      for file in pdf_files:
          print(file)

    return pdf_files

system_prompt = """
You are a highly skilled information extraction bot.
Your task is to extract specific information from the provided securities report PDF file.
Extract the following details and return them in JSON format:

- 종목명 (Stock Name)
- 종목코드 (티커) (Stock Code/Ticker)
- 작성일 (Date of Report)
- 현재 주가 (Current Stock Price - only numeric value)
- 목표 주가 (Target Stock Price - only numeric value)
- 투자 의견 (Investment Opinion - only in "Buy", "Hold" or "Sell")
- 작성 애널리스트 (Author Analyst)
- 소속 증권사 (Affiliated Securities Firm)

If a piece of information is not found, use 'N/A' for string values and 0 for numeric values.

Return only the JSON object. Do not include any other text.

Example JSON format:
{{
  "종목명": "Example Stock",
  "종목코드": "000000",
  "작성일": "YYYY-MM-DD",
  "현재 주가": 10000,
  "목표 주가": 12000,
  "투자 의견": "BUY",
  "작성 애널리스트": "Analyst Name",
  "소속 증권사": "Securities Firm Name"
}}
"""


def ask_gemini(directory_path:str, file_name:str, prompt:str=system_prompt, api_key:str=None, return_dict:bool=True, sleep:int=0) -> str:
    if api_key is None:
        api_key = os.getenv('GOOGLE_API_KEY')

    client = genai.Client(api_key=api_key)

    if file_name is not None:
        file_path = os.path.join(directory_path, file_name)

        try:
            # Upload the file using the genai client
            sample_file = client.files.upload(file=file_path)

            # Generate content using the uploaded file and the prompt
            response = client.models.generate_content(model="gemini-2.5-flash",
                                                      contents=[sample_file, prompt])

            result = response.text.replace("```json", "").replace("```", "")

            if sleep > 0:
                time.sleep(sleep)

            if return_dict:
                return json.loads(result)
            else:
                return result

        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print("No PDF files were selected for processing. Please run the previous cell.")

def find_target_hit_date(ticker:str, report_date:str, target_price:float):
    start_date = report_date.replace("-", "")
    end_date = datetime.today().strftime("%Y%m%d")

    df = stock.get_market_ohlcv_by_date(start_date, end_date, ticker)
    df = df[["종가"]]

    reached = df[df["종가"] >= target_price]

    if not reached.empty:
        first_hit_date = reached.index[0].strftime("%Y-%m-%d")

        report_dt = datetime.strptime(report_date, "%Y-%m-%d")
        hit_dt = datetime.strptime(first_hit_date, "%Y-%m-%d")

        return first_hit_date, (hit_dt - report_dt).days
    else:
        return None

def is_validate_report_data(report_data:dict) -> bool:
    """
    종목코드, 작성일, 목표 주가 => 필수
    종목명, 현재 주가, 투자 의견, 작성 애널리스트, 소속 증권사 => 선택
    """
    na_val = [None, "N/A", "n/a", "", 0]
    return report_data \
          and isinstance(report_data, dict) \
          and report_data.get("종목코드") not in na_val \
          and report_data.get("작성일") not in na_val \
          and report_data.get("목표 주가") not in na_val

def process_single_pdf(file_name:str, directory_path:str, feat_extractor:Callable, target_hitter:Callable, feat_extractor_kwargs:dict=None) -> dict:
    """
    Processes a single PDF file by extracting info and finding target hit date.
    """
    report_info = None
    hit_date_info = None

    try:
        feat_extractor_kwargs = {} if feat_extractor_kwargs is None else feat_extractor_kwargs
        # Task 1: Extract information using ask_gemini (I/O-bound)
        report_info = feat_extractor(directory_path, file_name, **feat_extractor_kwargs)

        # Check if essential info is available for target hit date calculation
        if is_validate_report_data(report_info):
            ticker = report_info["종목코드"]
            report_date = report_info["작성일"]
            target_price = report_info["목표 주가"]

            try:
                # Task 2: Find target hit date (potentially CPU-bound, but often quick with PyKRX)
                hit_date_info = target_hitter(ticker, report_date, target_price)
            except Exception as hit_error:
                hit_date_info = f"Error finding target hit date: {hit_error}"

            # Combine the results
            return {
                "pdf_file": file_name,
                "report_info": report_info, # Return report_info even if hit_date_info failed
                "hit_date_info": hit_date_info
            }
        else:
            return {
                "pdf_file": file_name,
                "report_info": report_info, # Return potentially incomplete report_info
                "hit_date_info": "Could not extract essential information for target hit date."
            }

    except Exception as extract_error:
        print(f"Error processing {file_name} during feature extraction: {extract_error}")
        return {
            "pdf_file": file_name,
            "report_info": None, # Return None if feature extraction failed
            "hit_date_info": f"Error during feature extraction: {extract_error}"
        }
    
def report_preprocessing_parallel_with_db(directory_path:str, get_files_fn:Callable, pipeline_fn:Callable, feat_extractor:Callable, target_hitter:Callable, feat_extractor_kwargs:dict=None,
                                          num_workers:int=5, verbose:bool=True, conn=None, cursor=None) -> tuple:
    """
    Parallel Processing Implementation for Report Preprocessing with direct DB insertion.
    Assumes DB connection 'conn' and cursor 'cursor' are available in the scope where this function is called.
    """
    if conn is None or cursor is None:
        raise ValueError("DB connection and cursor must be provided.")

    processed_results = []

    # Using ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Get the list of PDF files using the executor
        future_to_get_files = executor.submit(get_files_fn, directory_path)
        selected_pdf_files = future_to_get_files.result() # Wait for the file list to be ready

        if verbose:
            print(f"Starting parallel processing for {len(selected_pdf_files)} files...")

        # Submit tasks for processing each PDF file
        future_to_pdf = {executor.submit(pipeline_fn, pdf_file, directory_path, feat_extractor, target_hitter, feat_extractor_kwargs): pdf_file for pdf_file in selected_pdf_files}

        # Process the results as they complete
        for future in as_completed(future_to_pdf):
            pdf_file = future_to_pdf[future]
            try:
                result = future.result() # result like dict {"pdf_file": str, "report_info": json, "hit_date_info": tuple}
                processed_results.append(result)

                # --- Direct DB Insertion within the loop ---
                report_info = result.get("report_info")
                hit_date_info = result.get("hit_date_info") # tuple or None (normal) / str (abnormal)
                file_name = result.get("pdf_file")

                # Check if essential information was extracted successfully before attempting insert
                if is_validate_report_data(report_info):
                    # Prepare data for report_info table
                    # If a constraint error occurs, judge as a data error and passed
                    report_info_data = (
                        file_name,
                        report_info.get("종목명"),
                        report_info.get("종목코드"),
                        report_info.get("작성일"),
                        report_info.get("현재 주가"),
                        report_info.get("목표 주가"),
                        report_info.get("투자 의견").lower() == "buy",
                        report_info.get("작성 애널리스트"),
                        report_info.get("소속 증권사")
                    )

                    try:
                        cursor.execute("""
                            INSERT INTO report_info (pdf_file, stock, ticker, published_date, current_price, target_price, investment_opinion, author_analyst, affiliated_firm)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, report_info_data)

                        conn.commit()
                        if verbose:
                            print(f"Successfully extracted data for: {pdf_file}")
                            try:
                                shutil.move(os.path.join(pdf_path, pdf_file), os.path.join(pdf_finished_path, pdf_file))
                                print(f"Moved {pdf_file} with completely extracted data to: {pdf_finished_path}/")
                            except FileNotFoundError:
                                print(f"{pdf_file} not found in: {pdf_path}/")
                            except PermissionError:
                                print(f"No permission to move file: {pdf_file}")
                            except Exception as shutil_exc:
                                print(f"Failed to move file {pdf_file}: {shutil_exc}")

                    except (Exception, psycopg2.Error) as db_error_info:
                        conn.rollback()
                        print(f"REPORT_INFO table INSERT error for {pdf_file}: {db_error_info}")

                    if isinstance(hit_date_info, str) and hit_date_info.startswith("Error finding target hit date:"):
                        # If target_hitter occurs error alone, prevent insert None into REPORT_HIT.
                        # None in REPORT_HIT table means Hit miss, not error.
                        print(f"Error occurs only on target_hitter. REPORT_HIT table INSERT is passed for {pdf_file}: {hit_date_info}")
                    else:
                        # Prepare data for report_hit table
                        hit_date = hit_date_info[0] if isinstance(hit_date_info, tuple) else None
                        hit_days = hit_date_info[1] if isinstance(hit_date_info, tuple) else None

                        # If both hit_date and hit_days are None, judge as a Hit miss
                        report_hit_data = (
                            file_name,
                            hit_date,
                            hit_days
                        )

                        try:
                            cursor.execute("""
                                INSERT INTO report_hit (pdf_file, hit_date, hit_days)
                                VALUES (%s, %s, %s);
                            """, report_hit_data)

                            conn.commit()
                            if verbose:
                                print(f"Successfully processed and inserted data for: {pdf_file}")

                        except (Exception, psycopg2.Error) as db_error_hit:
                            conn.rollback()
                            print(f"REPORT_HIT table INSERT error for {pdf_file}: {db_error_hit}")
                            # Log this error or handle it as needed without stopping the loop

                else:
                    if verbose:
                         print(f"Skipping DB insert for {pdf_file}: Essential info missing or processing error.")

            except Exception as exc:
                print(f'{pdf_file} generated an exception during processing: {exc}')
                # This catches errors during the PDF processing pipeline_fn

    return processed_results, conn, cursor # You might still want to return results for logging or further processing



if __name__=="__main__":
    load_dotenv()
    
    if os.path.exists(pdf_path):
        print(f"디렉터리 '{pdf_path}'가 존재합니다.")
    else:
        print(f"디렉터리 '{pdf_path}'가 존재하지 않습니다.")

    # 연결 정보 설정
    try:
        # 데이터베이스 연결
        conn = psycopg2.connect(
            host="localhost",
            dbname=os.getenv('db_name'),
            user=os.getenv('db_user'),
            password=os.getenv('POSTGRES_KEY')
        )
        cursor = conn.cursor()

        # 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_info (
                pdf_file VARCHAR(100) PRIMARY KEY,
                stock VARCHAR(50),
                ticker VARCHAR(6) NOT NULL CHECK (LENGTH(ticker) = 6),
                published_date DATE NOT NULL,
                current_price INT,
                target_price INT NOT NULL,
                investment_opinion BOOLEAN,
                author_analyst VARCHAR(15),
                affiliated_firm VARCHAR(50)
            );
        """)
        conn.commit()
        print("REPORT_INFO 테이블이 성공적으로 생성되었습니다.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_hit (
                pdf_file VARCHAR(100) PRIMARY KEY,
                hit_date DATE,
                hit_days INT
            );
        """)
        conn.commit()
        print("REPORT_HIT 테이블이 성공적으로 생성되었습니다.\n")

        # Assuming pdf_path, get_report_pdf_files, process_single_pdf, ask_gemini are defined in previous cells
        # You might need to ensure these are defined or modify this part if they are not
        try:
            processed_results, conn, cursor = report_preprocessing_parallel_with_db(pdf_path,
                                                                                    get_report_pdf_files,
                                                                                    process_single_pdf,
                                                                                    ask_gemini,
                                                                                    find_target_hit_date,
                                                                                    #   num_workers=1,
                                                                                    conn=conn, cursor=cursor)
        except NameError as e:
            print(f"Error: Required functions or variables are not defined. Please ensure all preceding cells are executed. Details: {e}")


        # 데이터 조회
        cursor.execute("SELECT * FROM report_info;")
        rows = cursor.fetchall()
        print("\n================ 데이터 조회 ================", end="")
        print("\n테이블 데이터:")
        for row in rows:
            print(row)

        cursor.execute("SELECT * FROM report_hit;")
        rows = cursor.fetchall()
        print("\n테이블 데이터:")
        for row in rows:
            print(row)

        # 연결 종료
        cursor.close()
        conn.close()

    except (Exception, psycopg2.Error) as error:
        print(f"PostgreSQL 오류 발생: {error}")