from src.Modules.DuckDB import DuckDB
from src.Modules.Postgress import Postgres
from src.Modules.SQLite import SQLite
from src.Modules.SQLAlchemy import SQLAlchemy
from src.Modules.Pandas import Pandas


class Benchmark:
    def __init__(self, config):
        # Устанавливаем в базовом классе кол-во запусков и путь к файлу
        self.databases = config["databases"]
        self.config = config
        Postgres.host_ = config["postgres_conf"]["host"]
        Postgres.password_ = config["postgres_conf"]["password"]
        Postgres.user_ = config["postgres_conf"]["user"]
        Postgres.port_ = config["postgres_conf"]["port"]

    def tests(self):
        db_for_tests = []
        # Инстанциируем объекты классов, которые были заданы в конфиге
        for db in self.databases:
            if db == "Postgres":
                db_for_tests.append(Postgres(self.config["number_of_starts"],
                                             self.config["create_db_postgres"],
                                             self.config["path_to_file_csv"]))
            elif db == "SQLite":
                db_for_tests.append(SQLite(self.config["number_of_starts"],
                                           self.config["create_db_sqlite"],
                                           self.config["path_to_file_csv"],
                                           self.config["path_to_sqlite"]))
            elif db == "DuckDB":
                db_for_tests.append(DuckDB(self.config["number_of_starts"],
                                           self.config["path_to_file_csv"]))
            elif db == "SQLAlchemy":
                db_for_tests.append(SQLAlchemy(self.config["number_of_starts"],
                                               self.config["create_db_sqlite"],
                                               self.config["path_to_file_csv"]))
            elif db == "Pandas":
                db_for_tests.append(Pandas(self.config["number_of_starts"],
                                           self.config["path_to_file_csv"]))
        for db in db_for_tests:
            # Запускаем для каждой выбранной бд тесты
            db.start_test()
