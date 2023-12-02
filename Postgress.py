import time
from DataBase import DataBase
import statistics
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

class Postgress(DataBase):
    def __init__(self):
        super().__init__()
        self.connection = psycopg2.connect(dbname="postgres",
                                   host="127.0.0.1",
                                   user="postgres",
                                   password="230704",
                                   port="5432")
        engine = create_engine('postgresql+psycopg2://postgres:230704@localhost:5432/postgres')
        db = engine.connect()
        #df = pd.read_csv(DataBase.path_to_file)
        #df.to_sql('postgres', con=db, if_exists='replace', index=False)
        for df in pd.read_csv("C:\\Users\\vinog\\Downloads\\nyc_yellow_big.csv",sep='\t',iterator=True,chunksize=1000):
            df.to_sql('postgres', con=db, if_exists='replace', index=False)
        db.close()
    def startTest(self):
        self.connection = psycopg2.connect(dbname="postgres",
                                           host="127.0.0.1",
                                           user="postgres",
                                           password="230704",
                                           port="5432")
        self.connection.autocommit = True
        cursor = self.connection.cursor()

        super().startTest(cursor)

        cursor.close()
        self.connection.close()

        super().printTime("Postgress")
