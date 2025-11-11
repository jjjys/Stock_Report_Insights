# 실행 환경, 오버라이딩
from dotenv import load_dotenv
# from multipledispatch import dispatch

# 병렬처리
from concurrent.futures import ThreadPoolExecutor, as_completed

import traceback


load_dotenv()

# ------------------------------
# 공통 베이스 클래스 정의
# ------------------------------
class Node:
    """모든 노드의 공통 부모 클래스. 연산자로 연결 가능."""

    def __or__(self, other):
        # self 다음에 other를 실행하는 새 파이프라인을 반환
        return Pipeline([self, other])

    def __sub__(self, other):
        """self와 other를 하나의 노드로 연결하여 동시에 실행. 앞 노드 결과만 반환."""
        return Combined(self, other, hop_mode=True)

    def __add__(self, other):
        """self와 other를 하나의 노드로 연결하여 동시에 실행. 앞뒤 노드 결과 모두 반환."""
        return Combined(self, other, hop_mode=False)

    def __mul__(self, workers):
        """멀티스레딩을 쉽게 적용할 수 있는 연산자"""
        return MultiThreadNode(self, max_workers=workers)
    
    def __floordiv__(self, other):
        """다중 출력 노드와 단일 처리 노드를 연결"""
        return Pipeline([self, MapNode(other)])

    def __call__(self, data):
        """각 노드가 수행할 구체적 처리 로직 (자식 클래스에서 구현)"""
        raise NotImplementedError


class Combined(Node):
    """두 노드를 동시에 실행 (추출 + DB 적재)"""
    def __init__(self, left, right, hop_mode:bool):
        self.left = left
        self.right = right
        self.hop_mode = hop_mode

    def __call__(self, data):
        result_l = self.left(data)
        result_r = self.right(result_l)

        if self.hop_mode:
            return result_l  # 필요시 결과를 반환
        else:
            return result_l, result_r


class MultiThreadNode(Node):
    """내부 노드를 멀티스레딩으로 실행하는 노드"""
    def __init__(self, node, max_workers:int=4):
        self.node = node
        self.max_workers = max_workers

    def __call__(self, data_list:list|tuple):
        """data_list: 여러 입력 데이터를 동시에 처리"""
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.node, d): d for d in data_list}

            for future in as_completed(futures):
                data_item = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    print(f"[MultiThreadNode] '{data_item}' 처리 중 오류 발생: {e}")
                    traceback.print_exc(limit=1)
                    result = None  # 실패한 항목은 None 처리
                results.append(result)

        print("[MultiThreadNode] 모든 작업 완료")
        return results


class MapNode(Node):
    """리스트 형태의 입력을 받아 내부 노드를 각 항목별로 실행"""
    def __init__(self, node):
        self.node = node

    def __call__(self, data_list:list|tuple):
        if not isinstance(data_list, (list, tuple)):
            raise TypeError("MapNode는 리스트 형태의 입력만 처리할 수 있습니다.")

        print(f"[MapNode] {len(data_list)}개의 항목을 순차 처리 중...")
        results = []
        for i, d in enumerate(data_list, start=1):
            try:
                res = self.node(d)
                results.append(res)
            except Exception as e:
                print(f"[MapNode] {i}번째 항목 처리 중 오류 발생: {e}")
                results.append(None)

        print("[MapNode] 모든 작업 완료")
        return results


class Pipeline(Node):
    """여러 노드를 순차적으로 연결해 실행하는 클래스"""
    def __init__(self, nodes):
        self.nodes = []
        # 파이프라인 합성 지원
        for n in nodes:
            if isinstance(n, Pipeline):
                self.nodes.extend(n.nodes)
            else:
                self.nodes.append(n)

    def __or__(self, other):
        # Pipeline | Node 형태의 연결 지원
        return Pipeline(self.nodes + [other])

    def __call__(self, data):
        """파이프라인 실행"""
        value = data
        for node in self.nodes:
            value = node(value)

        return value