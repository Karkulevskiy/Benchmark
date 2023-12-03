from DataBase import *
from DuckDB import DuckDB
from Postgress import Postgress
from SQLite import SQLite
from SQLAlchemy import SQLAlchemy
from Pandas import Pandas

class Benchmark:
    def __init__(self, config):
        #Устанавливаем в базовом классе кол-во запусков и путь к файлу
        self.databases = config["databases"]
        DataBase.number_of_starts = config["number_of_starts"]
        DataBase.path_to_file = config["path_to_file"]
        Postgress.host_ = config["postgress_conf"]["host"]
        Postgress.password_ = config["postgress_conf"]["password"]
        Postgress.user_ = config["postgress_conf"]["user"]
        Postgress.port_ = config["postgress_conf"]["port"]
    def tests(self):
        db_for_tests = []
        #Инстанциируем объекты классов, которые были заданы в конфиге
        for db in self.databases:
            if db == "Postgress": db_for_tests.append(Postgress())
            elif db == "SQLite" : db_for_tests.append(SQLite())
            elif db == "DuckDB": db_for_tests.append(DuckDB())
            elif db == "SQLAlchemy": db_for_tests.append(SQLAlchemy())
            elif db == "Pandas": db_for_tests.append(Pandas())
        for db in db_for_tests:
            #Запускаем для каждой выбранной бд тесты
            db.startTest()