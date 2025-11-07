from crawlers.naver_crawler import NaverPaySecuritiesCrawler


if __name__ == "__main__":
    
    '''
    NaverPaySecuritiesCrawler 사용법 예시
    1. 기본 실행 (오늘 날짜 기준 크롤링)
    2. 특정 날짜 범위 지정하여 실행
    3. 크롤링 후 데이터 확인
    '''
    crawler = NaverPaySecuritiesCrawler()
    #crawler = NaverPaySecuritiesCrawler(start_date='2025-10-02', end_date='2025-10-02')
    #crawler = NaverPaySecuritiesCrawler(start_date='2021-02-16', end_date='2021-02-18')
    crawler.run()
    print(f"데이터 확인=========================")
    print(f"종목명:{crawler.data['종목분석 리포트']['data'][0]['종목명']}")
    print(f"제목:{crawler.data['종목분석 리포트']['data'][0]['제목']}")
    print(f"증권사:{crawler.data['종목분석 리포트']['data'][0]['증권사']}")
    print(f"Report_url:{crawler.data['종목분석 리포트']['data'][0]['Report_url']}")
    print(f"작성일:{crawler.data['종목분석 리포트']['data'][0]['작성일']}")
    print(f"조회수:{crawler.data['종목분석 리포트']['data'][0]['조회수']}")
    print(f"Report_local_path:{crawler.data['종목분석 리포트']['data'][0]['Report_local_path']}")
