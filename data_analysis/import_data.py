from sqlalchemy import create_engine
import pymysql
import os
from dotenv import load_dotenv
import pandas as pd

class DataImport():
    def __init__(self, database_name_: str, table_name_: str):
        self.table_name = table_name_
        self.database_name = database_name_
    
    def GetTable(self):   
        load_dotenv()
        pw = os.getenv('pw')
        CONN = create_engine("mysql+pymysql://" + "root" + ":" + pw + "@" + "localhost" + "/" + self.database_name)
        df = pd.read_sql(self.table_name, CONN)
        return df

    



