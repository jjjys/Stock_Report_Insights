from google import genai
from google.genai import types
import pathlib
import os


def ask(client, txt):
  response = client.models.generate_content(
      model="gemini-2.5-flash",
      contents="시부레 쇼부레 ㅠㅠ 나 힘들당",
      config=types.GenerateContentConfig(
          thinking_config=types.ThinkingConfig(thinking_budget=0) # Disables thinking
      ),
  )
  print(response.text)
  return response


def analyze_image(client, image_path):
  with open(image_path, 'rb') as f:
      image_bytes = f.read()

  response = client.models.generate_content(
  model='gemini-2.5-flash',
  contents=[
      types.Part.from_bytes(
      data=image_bytes,
      mime_type='image/jpeg',
      ),
      '이 이미지에서 보이는 숫자만 답변해줘.'
  ]
  )

  print(response.text)
  return response


def analyze_pdf(client, pdf_path):
  # Retrieve and encode the PDF byte
  filepath = pathlib.Path(pdf_path)

  prompt = "pdf 파일 핵심 요약 해줘."
  response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=[
        types.Part.from_bytes(
          data=filepath.read_bytes(),
          mime_type='application/pdf',
        ),
        prompt])
  
  print(response.text)
  return response

if __name__ == "__main__":
  GEMINI_API_KEY_01 = os.getenv("GEMINI_API_KEY_01")
  gemini_01 = genai.Client(api_key=GEMINI_API_KEY_01)
  
  image_path =  r'C:\Users\user\Desktop\sellking\data\captcha\captcha_20250721_151414.png' # 분석할 이미지 파일 경로
  pdf_path = r'C:\Users\user\Desktop\report\pdfs\종목분석_리포트\1H25_인건비와_관세_2H25_반등_25_05_16.pdf' # 분석할 PDF 파일 경로