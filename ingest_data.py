import pandas as pd
import glob
from sqlalchemy import create_engine

def csv_to_postgres(file, user, password, host, port, db_name ):
    #koneksi ke postgresql
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    #membaca file csv
    df = pd.read_csv(file)
    #import csv ke postgresql
    table_name = file.split("\\")[-1].split('.')[0]
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

def manycsv_to_postgres(path, user, password, host, port, db_name):
    #koneksi ke postgresql
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    #membaca file csv
    csv_files = glob.glob(path + "/*.csv")
    for f in csv_files:
        df = pd.read_csv(f)
        table_name = f.split("\\")[-1].split(".")[0] 
        #import csv ke postgresql
        df.to_sql(name= table_name, con=engine, if_exists="replace", index=False)
        print('Ingesting ', f.split("\\")[-1], ' file')

