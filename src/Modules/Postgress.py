import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import time
import os
import statistics

QUERY_1 = "SELECT \"VendorID\", count(*) FROM trips GROUP BY 1;"
QUERY_2 = '''SELECT passenger_count, avg(total_amount) 
FROM trips 
GROUP BY 1;'''
QUERY_3 = '''SELECT
   passenger_count, 
   extract(year from TO_TIMESTAMP(tpep_pickup_datetime,'YYYY/MM/DD/HH24:MI:ss')),
   count(*)
FROM trips
GROUP BY 1, 2;'''
QUERY_4 = '''SELECT
    passenger_count,
    extract(year from TO_TIMESTAMP(tpep_pickup_datetime,'YYYY/MM/DD/HH24:MI:ss')),
    round(trip_distance),
    count(*)
FROM trips
GROUP BY 1, 2, 3
ORDER BY 2, 4 desc;'''


class Postgres:
    host_ = ""
    user_ = ""
    password_ = ""
    port_ = ""

    def __init__(self, number_of_starts, create_db_postgres, path_to_file_csv):
        self.data = []
        self.QUERIES = [QUERY_1, QUERY_2, QUERY_3, QUERY_4]
        self.number_of_starts = number_of_starts
        self.create_db_postgres = create_db_postgres
        self.path_to_file_csv = path_to_file_csv
        if self.create_db_postgres and os.path.exists(path_to_file_csv):
            connection = psycopg2.connect(dbname='trips',
                                               host=self.host_,
                                               user=self.user_,
                                               password=self.password_,
                                               port=self.port_)
            engine = create_engine(
                f'postgresql+psycopg2://{str(self.user_)}:{str(self.password_)}@localhost:{str(self.port_)}/trips')
            db = engine.connect()
            df = pd.read_csv(self.path_to_file_csv)
            df.to_sql('trips', con=db, if_exists='replace', index=False, chunksize=1000, method='multi')
            db.close()

    def start_test(self):
        connection = psycopg2.connect(dbname='trips',
                                           host=self.host_,
                                           user=self.user_,
                                           password=self.password_,
                                           port=self.port_)

        connection.autocommit = True
        cursor = connection.cursor()

        for i in range(4):
            static_data = []
            for j in range(self.number_of_starts):
                start = time.time()
                cursor.execute(self.QUERIES[i])
                time_result = time.time() - start
                static_data.append(time_result)
            self.data.append(f"Query:{i + 1} ---> {str(statistics.median(static_data))}\n")

        cursor.close()
        connection.close()

        self.print_time("Postgres")

    def print_time(self, db_name):
        print(f"{db_name}: \n")
        for i in range(len(self.data)):
            print(self.data[i])
        with open("results.txt", "a") as file:
            file.write(f"{db_name}:\n" + "".join(self.data))
