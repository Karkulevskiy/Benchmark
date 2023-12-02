import json
import os
from Benchmark import *

CONFIG_NAME = "config.json"

def benchmark(config_data):
    bench = Benchmark(config_data)
    bench.tests()


def checkConfig():
    config_default = {"databases" : ["Postgress", "SQLite","Pandas","DuckDB","SQLAlchemy"], "number_of_starts" : 1, "path_to_file" : "C:\\Users\\vinog\\Downloads\\nyc_yellow_big.csv"}

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