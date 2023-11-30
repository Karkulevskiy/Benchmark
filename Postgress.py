import time
from DataBase import *
import psycopg2
import statistics

class Postgresql(DataBase):
    def __init__(self, number_of_starts):
        self.connection = psycopg2.connect(dbname="postgres",
                                   host="127.0.0.1",
                                   user="postgres",
                                   password="230704",
                                   port="5432")
        self.connection.autocommit = True
        self.startTest()

    def startTest(self):
        cursor = self.connection.cursor()
        for i in range(len(self.QUERIES)):
            static_data = []
            for j in range(10):
                start = time.time()
                cursor.execute(self.QUERIES[i])
                time_result = time.time() - start
                static_data.append(time_result)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")
        cursor.close()
        self.connection.close()
        self.printTime()

    def printTime(self):
        print("Postgress: \n")
        for i in range(len(self.data)):
            print(self.data[i])
        with open("results.txt", "a") as file:
            file.write("Postgres:\n" + "".join(self.data))

pg = Postgresql()