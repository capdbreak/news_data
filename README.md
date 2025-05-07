    News_Datas.parquet : [NVDA, AAPL, TSLA] 2024/05/01 ~ 2025/05/01 뉴스 데이터 모두 저장된 파일

    tickers.json : 우선 NVDA, AAPL, TSLA 세 종목에 대해 ticker와 그에 해당하는 검색 query저장

    NewsCountResults/ : 각 종목별로 2024/05/01 ~ 2025/05/01 기간동안 각각 몇개의 뉴스가 저장되었는지 .txt 파일로 저장

    parquet_to_postgreSQL.py : 원하는 parquet파일을 postgreSQL에 원하는 테이블 명으로 저장장

    get_news_googleRSS.py : 종목 ticker, 크롤링할 구간 입력 -> 구글 RSS로 유효한 뉴스 링크 추출 -> 추출한 링크에 대해 크롤링 후 저장

    외부 git 라이브러리 : https://github.com/SSujitX/google-news-url-decoder.git
git clone https://github.com/SSujitX/google-news-url-decoder.git
cd google-news-url-decoder
python -m pip install .



    description : 
1년치 데이터에 대해 크롤링하는데 종목별 시간이 오래 걸려서 parquet파일로 데이터를 우선 불러온 후 최신 데이터는 get_news_googleRSS.py를 이용해 자동으로 가져오도록 구축할 예정 ( 크롤링 주기, 종목 선택 등등 최종 조건이 정해질 경우 )