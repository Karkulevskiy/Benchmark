import time
from DataBase import DataBase
import statistics
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

class Postgress(DataBase):
    host_ = ""
    user_ = ""
    password_ = ""
    port_ = ""
    def __init__(self):
        super().__init__()
        self.connection = psycopg2.connect(dbname='postgres',
                                   host=self.host_,
                                   user=self.user_,
                                   password=self.password_,
                                   port=self.port_)
        engine = create_engine(f'postgresql+psycopg2://{str(self.user_)}:{str(self.password_)}@localhost:{str(self.port_)}/postgres')
        db = engine.connect()
        kostyl = True
        df = pd.read_csv(DataBase.path_to_file)
        df.to_sql('postgres',con=db, if_exists='replace',index=False, chunksize=1000,method='multi')
        # for df in pd.read_csv(DataBase.path_to_file, sep=',', iterator=True, chunksize=1000):
        #     if (kostyl):
        #         df.to_sql('postgres', con=db, if_exists='replace', index=False,  chunksize=1000, method='multi')
        #         kostyl = False
        #     df.to_sql('postgres', con=db, if_exists='append', index=False,  chunksize=1000, method='multi')
        db.close()
    def startTest(self):
        self.connection = psycopg2.connect(dbname='postgres',
                                       host=self.host_,
                                       user=self.user_,
                                       password=self.password_,
                                       port=self.port_)
        self.connection.autocommit = True
        cursor = self.connection.cursor()

        super().startTest(cursor)

        cursor.close()
        self.connection.close()

        super().printTime("Postgress")
