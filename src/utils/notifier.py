import requests
import json
from dotenv import load_dotenv
import os

# 웹훅 URL 불러오기
load_dotenv()  # .env 파일 로드
report_backup_WH = os.getenv("report_backup_WEB_HOOK")
target_price_notification_WH = os.getenv("target_price_notification_WEB_HOOK")

def send_discord_message(webhook_url, message='메세지 테스트'):
    """Discord로 메시지 전송하는 함수"""
    
    # Discord에 보낼 데이터
    data = {
        "content": message
        # "content": message,
        # "username": "테스트 봇"
    }
    
    # POST 요청으로 메시지 전송
    response = requests.post(
        webhook_url,
        data=json.dumps(data),
        headers={"Content-Type": "application/json"}
    )
    
    # 결과 확인
    if response.status_code == 204:
        print("✅ 메시지 전송 성공!")
    else:
        print(f"❌ 메시지 전송 실패: {response.status_code}")
        print(f"응답 내용: {response.text}")

def send_discord_file(webhook_url, file_path, message='파일 전송 테스트'):
    """Discord로 파일 전송하는 함수"""
    
    with open(file_path, 'rb') as file:
        files = {
            'file': (os.path.basename(file_path), file)
        }
        data = {
            "content": message
            # "content": message,
            # "username": "테스트 봇"
        }
        
        # POST 요청으로 파일 전송
        response = requests.post(
            webhook_url,
            data=data,
            files=files
        )
        
        # 결과 확인
        if response.status_code == 200:
            print("✅ 파일 전송 성공!")
        else:
            print(f"❌ 파일 전송 실패: {response.status_code}")
            print(f"응답 내용: {response.text}")

if __name__ == "__main__":
    # 테스트 메시지 전송
    send_discord_message(target_price_notification_WH, "안녕하세요! 이것은 테스트 메시지입니다.")
    
    # 테스트 파일 전송(이미지, PDF, 동영상 등)
    test_file_path = r"C:\Users\user\Desktop\Stock_Report_Insights\Stock_Report_Insights\data\reports\종목분석_리포트\251107_[BGF리테일]_3Q25_Review__격차를_줄여라.pdf"  # 전송할 파일 경로
    test_file_path = r"C://Users//user//Desktop//Stock_Report_Insights//Stock_Report_Insights//data//reports//종목분석_리포트//251107_[BGF리테일]_3Q25_Review__격차를_줄여라.pdf"  # 전송할 파일 경로
    send_discord_file(report_backup_WH, test_file_path, "테스트 파일을 전송합니다.")
    print("test 완료.")