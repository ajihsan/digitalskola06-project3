# IMPORT MODULE
import json
import pandas as pd
import numpy as np

# IMPORT SCRIPT
from script.mysql import MySQL
from script.postgresql import PostgreSQL

# IMPORT SQL
from sql.query import create_table_dim, create_table_fact


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

def create_star_schema(schema):
  postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
  conn, cursor = postgre_auth.connect(conn_type='cursor')

  query_dim = create_table_dim(schema=schema)
  cursor.execute(query_dim)
  conn.commit()

  query_fact = create_table_fact(schema=schema)
  cursor.execute(query_fact)
  conn.commit()

  cursor.close()
  conn.close()

def insert_dim_province(data):
    column_start = ["kode_prov", "nama_prov"]
    column_end = ["province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_district(data):
    column_start = ["kode_kab", "kode_prov", "nama_kab"]
    column_end = ["district_id", "province_id", "province_name"]

    data = data[column_start]
    data = data.drop_duplicates(column_start)
    data.columns = column_end

    return data


def insert_dim_case(data):
    column_start = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_end = ["id", "status_name", "status_detail", "status"]

    data = data[column_start]
    data = data[:1]
    data = data.melt(var_name="status", value_name="total")
    data = data.drop_duplicates("status").sort_values("status")
    
    data['id'] = np.arange(1, data.shape[0]+1)
    data[['status_name', 'status_detail']] = data['status'].str.split('_', n=1, expand=True)
    data = data[column_end]

    return data



def insert_raw_to_warehouse(schema):
    mysql_auth = MySQL(credential['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()
    data = pd.read_sql(sql='aji_raw_covid', con=engine)
    engine.dispose()

    # filter needed column
    column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    data = data[column]

    dim_province = insert_dim_province(data)
    dim_district = insert_dim_district(data)
    dim_case = insert_dim_case(data)

    postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')

    dim_province.to_sql(f'{schema}.dim_province', con=engine, index=False, if_exists='replace')
    dim_district.to_sql(f'{schema}.dim_district', con=engine, index=False, if_exists='replace')
    dim_case.to_sql(f'{schema}.dim_case', con=engine, index=False, if_exists='replace')

    engine.dispose()
if __name__ == '__main__':
  # insert_raw_data()
  create_star_schema(schema='aji')
  insert_raw_to_warehouse(schema='aji')