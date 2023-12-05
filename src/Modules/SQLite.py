import sqlite3
import pandas as pd
import time
import statistics
from sqlalchemy import create_engine
import os

QUERY_1 = "SELECT \"VendorID\", count(*) FROM trips GROUP BY 1;"
QUERY_2 = '''SELECT passenger_count, avg(total_amount) 
FROM trips 
GROUP BY 1;'''
QUERY_3 = '''SELECT
   passenger_count, 
   strftime("%Y", tpep_pickup_datetime),
   count(*)
FROM trips
GROUP BY 1, 2;'''
QUERY_4 = '''SELECT
    passenger_count,
    strftime("%Y", tpep_pickup_datetime),
    round(trip_distance),
    count(*)
FROM trips
GROUP BY 1, 2, 3
ORDER BY 2, 4 desc;'''


class SQLite:
    def __init__(self, number_of_starts, create_db_sqlite, path_to_file_csv, path_to_sqlite):
        self.data = []
        self.QUERIES = [QUERY_1, QUERY_2, QUERY_3, QUERY_4]
        self.number_of_starts = number_of_starts
        self.create_db_sqlite = create_db_sqlite
        self.path_to_file_csv = path_to_file_csv
        self.path_to_sqlite = path_to_sqlite
        if self.create_db_sqlite and os.path.exists(self.path_to_file_csv) and not os.path.exists("sqlite.db"):
            conn = sqlite3.connect("sqlite.db")
            engine = create_engine("sqlite:///sqlite.db")
            db = engine.connect()
            df = pd.read_csv(self.path_to_file_csv)
            df.to_sql('trips', con=db, if_exists='replace', index=False, chunksize=1000, method='multi')
            db.close()

    def start_test(self):
        connection = sqlite3.connect(self.path_to_sqlite)
        cursor = connection.cursor()

        for i in range(len(self.QUERIES)):
            static_data = []
            for j in range(self.number_of_starts):
                start = time.time()
                cursor.execute(self.QUERIES[i])
                time_result = time.time() - start
                static_data.append(time_result)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")

        cursor.close()
        connection.close()

        self.print_time("SQLite")

    def print_time(self, db_name):
        print(f"{db_name}: \n")
        for i in range(len(self.data)):
            print(self.data[i])
        with open("results.txt", "a") as file:
            file.write(f"{db_name}:\n" + "".join(self.data))
