import time
import statistics

QUERY_1 = "SELECT extra, count(*) FROM postgres GROUP BY 1;"
QUERY_2 = '''SELECT passenger_count, avg(total_amount) 
FROM postgres 
GROUP BY 1;'''
QUERY_3 = '''SELECT
   passenger_count, 
   extract(year from TO_TIMESTAMP(tpep_pickup_datetime,'YYYY/MM/DD/HH24:MI:ss')),
   count(*)
FROM postgres
GROUP BY 1, 2;'''
QUERY_4 = '''SELECT
    passenger_count,
    extract(year from TO_TIMESTAMP(tpep_pickup_datetime,'YYYY/MM/DD/HH24:MI:ss')),
    round(trip_distance),
    count(*)
FROM postgres
GROUP BY 1, 2, 3
ORDER BY 2, 4 desc;'''

class DataBase:
    QUERIES = [QUERY_1, QUERY_2, QUERY_3, QUERY_4]
    number_of_starts = 1
    path_to_file = ""

    def __init__(self):
        self.data = []
    def startTest(self,cursor):
        for i in range(len(self.QUERIES)):
            static_data = []
            for j in range(self.number_of_starts):
                start = time.time()
                cursor.execute(self.QUERIES[i])
                time_result = time.time() - start
                static_data.append(time_result)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")

    def printTime(self, dbName):
        print(f"{dbName}: \n")
        for i in range(len(self.data)):
            print(self.data[i])
        with open("results.txt", "a") as file:
            file.write(f"{dbName}:\n" + "".join(self.data))