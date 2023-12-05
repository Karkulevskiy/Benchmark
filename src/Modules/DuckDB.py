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


class DuckDB:

    def __init__(self, number_of_starts, path_to_file_csv):
        self.data = []
        self.QUERIES = [QUERY_1, QUERY_2, QUERY_3, QUERY_4]
        self.number_of_starts = number_of_starts
        self.path_to_file_csv = path_to_file_csv

    def start_test(self):
        if not os.path.exists(self.path_to_file_csv):
            print("File doesn't exist")
            return

        for i in range(4):
            self.QUERIES[i] = self.QUERIES[i].replace("__replace__", self.path_to_file_csv)

        duckdb.read_csv(self.path_to_file_csv)

        for i in range(4):
            static_data = []
            for j in range(1):
                start = time.time()
                duckdb.execute(self.QUERIES[i])
                time_result = time.time() - start
                static_data.append(time_result)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")
        self.print_time("DuckDB")

    def print_time(self, db_name):
        print(f"{db_name}: \n")
        for i in range(len(self.data)):
            print(self.data[i])
        with open("results.txt", "a") as file:
            file.write(f"{db_name}:\n" + "".join(self.data))
