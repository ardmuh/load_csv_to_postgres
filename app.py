import ingest_data

if __name__ == "__main__":
    
    print('Starting the ingestion...')
    #Mengimport satu file csv ke Postgresql
    # data = ingest_data.csv_to_postgres('data\customer_data_history.csv', 'postgres', '0078', 'localhost', 5432, 'test_db' )

    #Mengimport banyak file csv ke Postgresql
    data2 = ingest_data.manycsv_to_postgres('data', 'postgres', '0078', 'localhost', 5432, 'test_db' )
    print('The ingestion finished')
