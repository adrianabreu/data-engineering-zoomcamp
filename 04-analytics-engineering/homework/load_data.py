import os
import requests
import pandas as pd
from sqlalchemy import create_engine

years = [2019]
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
datasets = ['fhv']
def get_url(year, month, dataset):
    return f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{dataset}/{dataset}_tripdata_{year}-{month}.csv.gz"

# Define schemas
schemas = {
    "green": "green_tripdata",
    "yellow": "yellow_tripdata",
    "fhv": "fhv_tripdata"
}

# green => lpep
# yellow => tpep
# fhv => pickup_datetime,dropOff_datetime

# Function to download and extract files
def download_file(url, destination):
    response = requests.get(url)
    with open(destination, 'wb') as f:
        f.write(response.content)

# Function to insert data into database
def insert_data(schema, file_path, table_name, engine):
    df_iter = pd.read_csv(file_path, compression='gzip',  iterator=True, chunksize=100000)
    full_table_name = f"{schema}.{table_name}"
    df = next(df_iter)
    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='append')
    df.to_sql(table_name, schema=schema, if_exists='append', index=False, con = engine)
    while True:
        try:
            
            df = next(df_iter)
            df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
            df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
            df.to_sql(table_name, schema=schema, if_exists='append', index=False, con = engine)

            print(f"Data inserted into table {full_table_name}")
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

# Download and insert data into PostgreSQL
engine = create_engine('postgresql://root:root@127.0.0.1:5432/production')
for year in years:
    for month in months:
        for dataset in datasets:
            print(f"Processing dataset: {dataset}")
            table = schemas[dataset]
            file_path = f"{dataset}{year}{month}.csv.gz"
            url = get_url(year, month, dataset)
            # Download file
            print(f"Downloading file... {url}")
            download_file(url, file_path)
            
            # Insert data into database
            print("Inserting data into database...")
            insert_data('ny_taxi', file_path, table, engine)
                        
        

print("All data has been processed and inserted into the database.")