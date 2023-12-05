import sqlite3;
from src.DataBase import *
import pandas as pd
import time
import statistics
from sqlalchemy import create_engine
import os

QUERY_1 = "SELECT \"VendorID\", count(*) FROM postgres GROUP BY 1;"
QUERY_2 = '''SELECT passenger_count, avg(total_amount) 
FROM postgres 
GROUP BY 1;'''
QUERY_3 = '''SELECT
   passenger_count, 
   strftime("%Y", tpep_pickup_datetime),
   count(*)
FROM postgres
GROUP BY 1, 2;'''
QUERY_4 = '''SELECT
    passenger_count,
    strftime("%Y", tpep_pickup_datetime),
    round(trip_distance),
    count(*)
FROM postgres
GROUP BY 1, 2, 3
ORDER BY 2, 4 desc;'''

QUERIES = [QUERY_1, QUERY_2, QUERY_3, QUERY_4]

class SQLite(DataBase):
    def __init__(self):
        super().__init__()
        if DataBase.create_sqlite and os.path.exists(DataBase.path_to_file_csv):
            conn = sqlite3.connect("sqlite.db")
            engine = create_engine("sqlite:///sqlite.db")
            db = engine.connect()
            df = pd.read_csv(DataBase.path_to_file_csv)
            df.to_sql('postgres', con=db, if_exists='replace', index=False, chunksize=1000, method='multi')
            db.close()

    def startTest(self):
        connection = sqlite3.connect(DataBase.path_to_file_sqlite)
        cursor = connection.cursor()

        #Потому что SQLite не поддерживает функцию EXTRACT
        self.QUERIES = QUERIES

        for i in range(len(self.QUERIES)):
            static_data = []
            for j in range(DataBase.number_of_starts):
                start = time.time()
                cursor.execute(self.QUERIES[i])
                time_result = time.time() - start
                static_data.append(time_result)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")

        cursor.close()
        connection.close()

        super().printTime("SQLite")