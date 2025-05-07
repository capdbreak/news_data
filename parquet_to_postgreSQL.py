import pandas as pd
from sqlalchemy import create_engine

# 변환하려는 parquet파일 이름 및 경로 입력
parquet_file_path = "News_Datas.parquet"
df = pd.read_parquet(parquet_file_path)

# postgreSQL 접속 정보 입력
db_user = "user"
db_password = "password"
db_host = "localhost"
db_port = "5432"
db_name = "news"

db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# 테이블명 입력
table_name = "News_Datas"
df.to_sql(table_name, engine, if_exists="replace", index=False)

print(f"'{parquet_file_path}' 파일을 PostgreSQL 테이블 '{table_name}'에 저장")
