import pandas as pd
from sqlalchemy import create_engine
import psycopg2

connection = (psycopg2.connect(dbname="postgres",
                               host="127.0.0.1",
                               user="postgres",
                               password="230704",
                               port="5432"))


str = 'postgresql+psycopg2://postgres:230704@localhost:5432/postgres'

engine = create_engine(str)

db = engine.connect()

df = pd.read_csv("C:\\Users\\vinog\Downloads\\nyc_yellow_tiny.csv")

df.to_sql('postgres', con=db ,if_exists='replace',index=False)
#df.to_sql('postgres', connection, if_exists='replace', index=False)

db.close()