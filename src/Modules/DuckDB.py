from src.DataBase import *
import duckdb
import time
import statistics
import os

QUERY_1 = "SELECT \"VendorID\", count(*) FROM '__replace__' GROUP BY 1;"
QUERY_2 = '''SELECT passenger_count, avg(total_amount) 
FROM '__replace__' 
GROUP BY 1;'''
QUERY_3 = '''SELECT
   passenger_count, 
   extract(year from tpep_pickup_datetime),
   count(*)
FROM '__replace__'
GROUP BY 1, 2;'''
QUERY_4 = '''SELECT
    passenger_count,
    extract(year from tpep_pickup_datetime),
    round(trip_distance),
    count(*)
FROM '__replace__'
GROUP BY 1, 2, 3
ORDER BY 2, 4 desc;'''

QUERIES = [QUERY_1,QUERY_2,QUERY_3,QUERY_4]
class DuckDB(DataBase):

    def __init__(self):
        super().__init__()

    def startTest(self):
        if os.path.exists(DataBase.path_to_file_csv) == False:
            print("File doesn't exist")
            return

        for i in range(4):
            QUERIES[i] = QUERIES[i].replace("__replace__", DataBase.path_to_file_csv)

        duckdb.read_csv(DataBase.path_to_file_csv)

        for i in range(4):
            static_data = []
            for j in range(1):
                start = time.time()
                duckdb.execute(QUERIES[i])
                time_result = time.time() - start
                static_data.append(time_result)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")
        super().printTime("DuckDB")

