import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    filename="logs/ingestion_db.log",  
    filemode='a'          
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name,engine):
    df.to_sql(table_name,con = engine, if_exists = 'replace',  index = True)


def load_raw_data():
    start = time.time()
    for file in os.listdir('dataset'):
        if file.endswith('.csv'):
            df = pd.read_csv('dataset/'+file)
            logging.info(f'ingesting {file} in db')
            ingest_db(df,file[:-4],engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('-----------Ingestion Complete-----------')
    logging.info(f"\nTotal Time Taken : {total_time} minutes")

if __name__ == '__main__':
    load_raw_data()