import pandas as pd
from argparse import ArgumentParser
from sqlalchemy import create_engine

parser = ArgumentParser()

parser.add_argument("--job_type", type=str, default='start')
parser.add_argument("--root_password", type=str, required=True)

args = parser.parse_args()
engine = create_engine(f'mysql+pymysql://root:{args.root_password}@localhost/source')
connection = engine.connect()


def start_data():
    df = pd.read_excel('./datasets/Financial Sample.xlsx')
    df.to_sql('financial', con = connection, schema='source')

