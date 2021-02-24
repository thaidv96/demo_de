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


def extract_data():
    df = pd.read_excel('./datasets/Financial Sample.xlsx')
    df.columns = [i.strip().lower().replace(" ","_") for i in df.columns]
    df.reset_index(inplace=True)
    df.rename(columns = {"index": 'id'})
    input_data_size = df.shape[0]
    df.to_sql('financial', con = stagging_connection, schema='stagging',if_exists='replace')

def gen_simple_dim_select_sql(*fields, table='financial'):
    return f"""Select distinct {','.join(fields)} from {table}"""


def create_dim(*cols, nammings=None,table_name=None,connection):
    sql = gen_simple_dim_select_sql(*cols)
    dim_df = pd.read_sql(sql, con=connection)
    dim_df.reset_index(inplace=True)
    dim_df.rename(columns={"index":"id"}, inplace=True)
    if nammings:
        dim_df.rename(columns={col:name for  col, name in zip(cols, nammings)}, inplace=True)
    dim_df.to_sql(table_name,con=connection, schema='stagging', if_exists='replace', index=False)


def create_dims():
    create_dim('product','manufacturing_price', nammings = ['name','manufacturing_price'], table_name='dim_product', connection=stagging_connection)
    create_dim('segment', nammings=['name'],table_name='dim_segment', connection=stagging_connection)
    create_dim('country', nammings=['name'],table_name='dim_country', connection=stagging_connection)
    create_dim('date','month_number','month_name','year', table_name='dim_date', connection=stagging_connection)
    create_dim('discount_band',nammings=['value'] ,table_name='dim_discount_band', connection=stagging_connection)

def create_facts():
    sql = """
    SELECT s.id segment_id, c.id country_id, 
    p.id product_id, db.id discount_band_id, 
    f.units_sold, f.sale_price, f.gross_sales, f.discounts,
    f.sales, f.cogs, f.profit, d.id date_id
    from financial f
    left join dim_segment s on s.name = f.segment
    left join dim_country c on c.name = f.country
    left join dim_product p on p.name = f.product
    left join dim_discount_band db on db.value = f.discount_band
    left join dim_date d on d.date = f.date
    """

    df = pd.read_sql(sql, con=stagging_connection)
    df.reset_index(inplace=True)
    df.rename(columns = {"index": "id"}, inplace=True)
    df.to_sql('fact_financial', con=stagging_connection, schema='stagging', if_exists='replace',index=False)

def load_dims():
    dim_tables = ['dim_segment','dim_country', 'dim_product','dim_discount_band', 'dim_date']
    for table in dim_tables:
        sql = f'select * from {table}'
        df = pd.read_sql(sql, con=stagging_connection)
        df.to_sql(table, con = target_connection,index=False, if_exists='replace', schema='target')

def load_facts():
    fact_tables = ['fact_financial']:
    for table in fact_tables:
        sql = f'select * from {table}'
        df = pd.read_sql(sql, con=stagging_connection)
        df.to_sql(table, con = target_connection,index=False, if_exists='replace', schema='target')


def etl():
    print("EXTRACTING...")
    extract_data()
    print("DATA EXTRACTED.\nTRANSFORMING...")
    create_dims()
    create_facts()
    print("DATA TRANSFORMED.\nLOADING...")
    load_dims()
    load_facts()
    print("DATA LOADED")
    print("\n\nDONE")




if __name__ == '__main__':
    if args.job_type == 'etl':
        etl()    

