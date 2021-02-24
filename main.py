import pandas as pd
from argparse import ArgumentParser
from sqlalchemy import create_engine
from datetime import datetime

parser = ArgumentParser()

parser.add_argument("--job_type", type=str, default='start')
parser.add_argument("--root_password", type=str, required=True)

args = parser.parse_args()
engine = create_engine(f'mysql+pymysql://root:{args.root_password}@localhost/source')
connection = engine.connect()


def start_data():
    print("START INGESTING DATA TO SOURCE...")
    df = pd.read_excel('./datasets/Financial Sample.xlsx')
    df.columns = [i.lower().replace(" ","_") for i in df.columns]
    input_data_size = df.shape[0]
    df.to_sql('financial', con = connection, schema='source',if_exists='replace')
    ## validate ingesting data 
    sql = "Select count(1) as num_records from source.financial"
    check_result = pd.read_sql(sql, con = connection)['num_records'].values[0]
    print("Input Data Size: ", input_data_size)
    print("Ingested Data Size:", check_result)
    with open("ingest_start_data.log",'a+') as f:
        f.write(f"{datetime.now()},{input_data_size},{check_result}\n")
    print("SUCCESS")



if __name__ == '__main__':
    if args.job_type == 'start':
        start_data()


