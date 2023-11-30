import time


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
    data = []
    def ___init___(self,number_of_starts):
        self.number_of_starts = number_of_starts

    def __init__(self):
        pass
    def printResult(self):
        pass
    def startTest(self):
        pass

