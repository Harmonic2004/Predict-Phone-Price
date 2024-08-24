import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql.functions import col, to_json

def save_data(data):

    # engine = create_engine('postgresql://postgres:@localhost:5432/Big-Data-Project')

    # data.to_sql('Phone', engine, if_exists='replace', index=False)

    # print("data stored in postgresql")
    # Tạo kết nối đến PostgreSQL
    # Chuyển đổi cột vector thành chuỗi JSON
    
    url = 'jdbc:postgresql://localhost:5432/postgres'
    properties = {
        'user': 'postgres',
        'password': '0918273645',
        'driver': 'org.postgresql.Driver'
    }

    # Lưu DataFrame vào PostgreSQL
    data.write.jdbc(url=url, table='Phone', mode='overwrite', properties=properties)