# IMPORT MODULE
import json
import pandas as pd

# IMPORT SCRIPT
from script.mysql import MySQL
from script.postgresql import PostgreSQL


with open ('credential.json', "r") as cred:
        credential = json.load(cred)

def insert_raw_data():
  mysql_auth = MySQL(credential['mysql_lake'])
  engine, engine_conn = mysql_auth.connect()

  with open ('./data/data_covid.json', "r") as data:
    data = json.load(data)

  df = pd.DataFrame(data['data']['content'])

  df.columns = [x.lower() for x in df.columns.to_list()]
  df.to_sql(name='aji_raw_covid', con=engine, if_exists="replace", index=False)
  engine.dispose()

if __name__ == '__main__':
  insert_raw_data()