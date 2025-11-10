from cores.cores import Node

from google import genai
import os, time, json


# ------------------------------
# 각 노드 정의 및 구현
# ------------------------------
class DocumentsLoader(Node):
    def __init__(self, docs_dir_path:str=None, extension:str=None, verbose:bool=False):
        """
        Args:
            docs_dir_path (str): 참고문서 파일이 있는 디렉토리 경로
            extension (str): 참고문서 파일 확장자
            verbose (bool): 진행상황 파악 여부
        """
        self.docs_dir_path = docs_dir_path
        self.extension = extension if extension is not None else "PDF"
        self.verbose = verbose

    def __call__(self, *args, **kwargs) -> list:
        """
        경로에서 참고문서 파일 목록을 가져옴

        Returns: 참고문서 파일명 (list)
        """
        print(f"[Files Loader] 디렉토리 경로: {self.docs_dir_path}")
        all_items = os.listdir(self.docs_dir_path)
        doc_files = [item for item in all_items if item.endswith("".join([".", self.extension.lower()]))]

        if self.verbose:
          print(f"다음 {self.extension.upper()} 파일이 로드됩니다:")
          for file in doc_files:
              print(file)

        return doc_files


class LLMFeatsExtractor(Node):
    def __init__(self, docs_dir_path:str, llm_type:str, llm_version:str, prompt:str, interval:int|float=0, api_key:str=None, essential_cols:list|tuple=None):
        """
        Args:
            docs_dir_path (str): 참고문서 디렉토리 경로 (DocumentsLoader 사용 시 경로 일치 필수)
            llm_version (str): 생성자 LLM 버전 정보 - 포맷은 모델에 따라 다름 (공식문서 참조)
            prompt (str): LLM에 전달할 프롬프트
            interval (int|float): LLM API 호출 간격 (초)
            api_key (str): LLM API 키 (로컬 모델의 경우 필요 없음)
            essential_cols (list|tuple): 추출 항목 중 필수 항목명
        """
        self.docs_dir_path = docs_dir_path
        self.llm_type = llm_type.lower()
        self.llm_version = llm_version.lower()
        self.prompt = prompt
        self.interval = interval
        self.api_key = api_key
        self.essential_cols = essential_cols if essential_cols is not None else tuple()

        self.model_map = {"gemini": self.call_gemini,}# "llama": self.call_llama, "qwen": self.call_qwen} # 모델명 + 메소드 매핑
        self.na_items = (None, "N/A", "n/a", "", 0) # 추출 실패 시 발생 항목

    def __call__(self, doc:str, *args, **kwargs) -> dict:
        """
        단일 참고문서에서 LLM을 통해 필요한 정보를 추출

        Args:
            doc (str): 단일 참고문서 파일명
        Returns: 참고문서에서 추출된 정보 (dict)
        """
        print(f"[LLMFeatsExtractor] {self.llm_type}(으)로 데이터 추출 중... : {doc}")
        extractor = self.model_map.get(self.llm_type, None)
        file_path = os.path.join(self.docs_dir_path, doc)

        if extractor is None:
            raise ValueError(f"지원하지 않는 LLM 타입: {self.llm_type}")

        if self.interval > 0:
            time.sleep(self.interval)

        try:
            response = extractor(file_path, self.llm_version, self.prompt, self.prompt, self.api_key)

            if self.is_valid_response(response):
                response["report_name"] = os.path.basename(file_path)
                response["llm_type"] = self.llm_type
                response["llm_version"] = self.llm_version

                print(f"[LLMFeatsExtractor] {self.llm_type} 추출 성공 !!")
                return response
            else:
                raise ValueError(f"{doc} 필수 데이터 없음: {response}")

        except Exception as e:
            print(f"[LLMFeatsExtractor] {self.llm_type}-{self.llm_version} Error: {e}")

    def is_valid_response(self, response:dict) -> bool:
        is_valid = response is not None and isinstance(response, dict)

        for ec in self.essential_cols:
            is_valid = is_valid and response.get(ec) not in self.na_items

        return is_valid

    def call_gemini(self, file_path:str, llm_version:str, prompt:str, interval:int|float=0, api_key:str=None) -> dict:
        client = genai.Client(api_key=api_key)
        sample_file = client.files.upload(file=file_path)

        response = client.models.generate_content(model=f"gemini-{llm_version}", contents=[sample_file, prompt])
        response = response.text.replace("```json", "").replace("```", "").strip()

        return json.loads(response)

    def call_llama(self) -> dict:
        pass

    def call_qwen(self) -> dict:
        pass