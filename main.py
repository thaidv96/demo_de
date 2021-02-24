import pandas as pd
from argparse import ArgumentParser
from sqlalchemy import create_engine
from datetime import datetime

parser = ArgumentParser()

parser.add_argument("--job_type", type=str, default='etl', required=False)
parser.add_argument("--root_password", type=str, required=True)

args = parser.parse_args()
engine = create_engine(f'mysql+pymysql://root:{args.root_password}@localhost/stagging')
stagging_connection = engine.connect()
target_engine = create_engine(f'mysql+pymysql://root:{args.root_password}@localhost/target')
target_connection = target_engine.connect()

def verify_data_ingestion():
    pass


def start_data():
    print("START INGESTING DATA TO STAGGING...")
    df = pd.read_excel('./datasets/Financial Sample.xlsx')
    df.columns = [i.lower().replace(" ","_") for i in df.columns]
    df.reset_index(inplace=True)
    df.rename(columns = {"index": 'id'})
    input_data_size = df.shape[0]
    df.to_sql('stagging_financial', con = stagging_connection, schema='stagging',if_exists='replace')
    print("SUCCESS LOAD DATA TO STAGGING AREA")

def gen_simple_dim_select_sql(*fields, table='stagging_financial'):
    return f"""Select distinct {','.join(fields)} from {table}"""


def etl_dim(*cols,table_name=None,connection):
    print(f"GENERATING {table_name} IN STAGGING AREA...")
    sql = gen_simple_dim_select_sql(*cols)
    dim_df = pd.read_sql(sql, con=connection)
    dim_df.reset_index(inplace=True)
    dim_df.rename(columns={"index":"id"}, inplace=True)
    dim_df.to_sql(table_name,con=connection, schema='stagging', if_exists='replace', index=False)
    print("DONE")


def etl_dims():
    start_data()
    etl_dim('product','manufacturing_price', table_name='dim_product', connection=stagging_connection)
    etl_dim('segment', table_name='dim_segment', connection=stagging_connection)
    etl_dim('country', table_name='dim_country', connection=stagging_connection)
    etl_dim('date','month_number','month_name','year', table_name='dim_date', connection=stagging_connection)
    etl_dim('discount_band', table_name='dim_discount_band', connection=stagging_connection)




def etl():

    etl_dims()

if __name__ == '__main__':
    if args.job_type == 'etl':
        etl()    

