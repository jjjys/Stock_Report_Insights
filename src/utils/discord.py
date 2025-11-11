import requests
import json
from dotenv import load_dotenv
import os


# 프로젝트 루트 경로 설정
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

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
        delete_file(file_path)
    else:
        print(f"❌ 파일 전송 실패: {response.status_code}")
        print(f"응답 내용: {response.text}")
    return response.status_code

def delete_file(file_path):
    """파일 삭제 함수"""    
    print(f"처리 파일: {file_path}")
    try:
        os.remove(file_path)
        print(f"🗑️ 파일 삭제 성공")
    except Exception as e:
        print(f"❌ 파일 삭제 실패: {e}")

def get_file_list(directory_path):
    """지정된 디렉토리의 파일 경로를 반환하는 함수"""
    try:
        # 디렉토리 내 파일 경로 리스트 추출
        file_list = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, file))]
        return file_list
    except FileNotFoundError:
        print(f"❌ 경로를 찾을 수 없습니다: {directory_path}")
        return []
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return []
    
if __name__ == "__main__":
    ############## 웹훅 URL 불러오기
    load_dotenv()  # .env 파일 로드
    report_backup_WH = os.getenv("report_backup_WEB_HOOK")
    target_price_notification_WH = os.getenv("target_price_notification_WEB_HOOK")
    
    ############## 메시지 전송
    #send_discord_message(target_price_notification_WH, "안녕하세요! 이것은 테스트 메시지입니다.")
    
    ############## 파일 전송(이미지, PDF, 동영상 등)
    #test_file_path = r"C:\Users\user\Desktop\Stock_Report_Insights\Stock_Report_Insights\data\reports\종목분석_리포트\251107_[BGF리테일]_3Q25_Review__격차를_줄여라.pdf"  # 전송할 파일 경로
    #test_file_path = r"C://Users//user//Desktop//Stock_Report_Insights//Stock_Report_Insights//data//reports//종목분석_리포트//251107_[BGF리테일]_3Q25_Review__격차를_줄여라.pdf"  # 전송할 파일 경로
    #send_discord_file(report_backup_WH, test_file_path, "Discord 레포트 전송 후 삭제 완료.")

    ############## processed(처리 완료 경로)에 있는 파일 처리(Discord 전송 및 삭제).
    #base_dir = r"C://Users//user//Desktop//Stock_Report_Insights//Stock_Report_Insights//data//processed"  # 전송할 파일 경로
    base_dir = os.path.join(PROJECT_ROOT, "data", "processed")
    processed_list = get_file_list(base_dir)
    if not processed_list:  # 파일 리스트가 비어 있는 경우
        print("📂 처리할 파일이 없습니다.")
    else:
        for idx, file_path in enumerate(processed_list):
            send_discord_file(
                report_backup_WH,
                file_path,
                f"처리된 파일({idx+1}/{len(processed_list)}) Discord 전송 후 삭제 완료.\n처리된 파일명:{file_path}")
            print(f"📤 진행 상황: {idx+1}/{len(processed_list)} 파일 처리 완료")

    print("test 완료.")