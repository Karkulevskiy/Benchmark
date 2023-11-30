import json
import os
import Benchmark

CONFIG_NAME = "config.json"

def benchmark(config_data):
    bench = Benchmark(config_data)


def checkConfig():
    config_default = {"databases" : ["Postgress", "SQLite", "DuckDB", "Pandas", "SQLAlchemy"], "number_of_starts" : 10}

    if (os.path.exists(CONFIG_NAME) == False):
        with open(CONFIG_NAME,"w+"):
            pass

    with open("config.json","r") as file:
        template = file.readline()
        if template:
            benchmark(template)
            return
        else:
            file.close()
            with open("config.json","w") as file:
                json.dump(config_default,file)
                benchmark(config_default)

checkConfig()