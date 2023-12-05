import json
import os
from src.Benchmark import *

CONFIG_NAME = "config.json"

def benchmark(config_data):
    bench = Benchmark(config_data)
    bench.tests()


def checkConfig():
    config_default = {"databases" : ["Postgress", "SQLite","Pandas","DuckDB","SQLAlchemy"],
                      "number_of_starts" : 1,
                      "path_to_file_sqlite" : "sqlite.db",
                      "path_to_file_csv" : "C:\\Users\\vinog\\Downloads\\nyc_yellow_tiny.csv",
                      "create_postgres" : False,
                      "create_sqlite" : False,
                      "postgress_conf" : {
                          "host" : "127.0.0.1",
                          "user" : "postgres",
                          "password" : "230704",
                          "port" : "5432"
                      }}

    if (os.path.exists(CONFIG_NAME) == False):
        with open(CONFIG_NAME,"w+"):
            pass

    with open(CONFIG_NAME,"r") as file:
        template = file.readline()
        if len(template) != 0:
            benchmark(json.loads(template))
        else:
            file.close()
            with open(CONFIG_NAME,"w") as file:
                json.dump(config_default,file)
                benchmark(config_default)

checkConfig()