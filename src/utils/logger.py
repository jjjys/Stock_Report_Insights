import logging
import os, sys
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

# class StreamToLogger:
#     """print() 출력을 logging으로 리디렉션"""
#     def __init__(self, logger, log_level=logging.INFO):
#         self.logger = logger
#         self.log_level = log_level
#         self.linebuf = ''

#     def write(self, buf):
#         for line in buf.rstrip().splitlines():
#             self.logger.log(self.log_level, line.rstrip())

#     def flush(self):
#         pass  # logging이 자체적으로 처리함


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # DEBUG로 설정

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler(f"logs/app_{datetime.now().strftime('%Y_%m_%d')}.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 표준 입출력 로그
    # sys.stdout = StreamToLogger(logger, logging.INFO)
    # sys.stderr = StreamToLogger(logger, logging.ERROR)

    return logger


def log_function(level=logging.INFO):
    """함수 호출 시 자동 로깅 데코레이터."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            func_name = func.__name__   # 함수명 (또는 메서드명)

            # 클래스명 추론
            cls_name = None
            if args:
                instance = args[0]
                if hasattr(instance, "__class__"):  # 첫 번째 인자가 self인 경우
                    cls_name = instance.__class__.__name__

            full_name = f"{cls_name}.{func_name}" if cls_name else func_name
            logger.log(level, f"{full_name} [PARAMS]: {args}, {kwargs}")
            
            try:
                result = func(*args, **kwargs)
                logger.log(level, f"{full_name} [RETURN]: {result}")
                return result
            except Exception as e:
                logger.error(f"Exception in {full_name}: {e}", exc_info=True)
                raise
            
        return wrapper
    return decorator
