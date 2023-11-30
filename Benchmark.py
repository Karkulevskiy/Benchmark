from datetime import time
from Postgress import *


class Benchmark:
    def __init__(self, config):
        self.databases = {}
        self.config = config
        self.number_of_starts = 10
        for data in self.databases.values():
            if   data.isdigit(): self.number_of_starts = data
            elif data == "Posgress": self.databases["Postgress"] = Postgress()
            elif data == "SQLite": self.databases["SQLite"] = SQLite()
            elif data == "DuckDB": self.databases["DuckDB"] = DuckDB()
            elif data == "Pandas": self.databases["Pandas"] = Pandas()
            elif data == "SQLAlchemy": self.databases["SQLAlchemy"] = SQLAlchemy()
        self.tests()

    def tests(self):
        values = self.config.values()
        for i in range(len(values) - 1):
            database = self.databases[values[i]]
            database.startTest()