from cores import Node


# --------------------------
# 사용자 정의 노드
# --------------------------
class DataProcessor(Node):
    def __call__(self, data, *args, **kwargs):
        print("[DataProcessor] 사용자 정의 노드 구현용...")
        raise NotImplementedError