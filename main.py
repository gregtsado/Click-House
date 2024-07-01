from helpers import get_client, get_postgres_engine
from extract import fetch_data
from load import load_csv_to_postgres
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

query ="""
        SELECT pickup_date, vendor_id, passenger_count, trip_distance, payment_type, fare_amount, tip_amount
        FROM tripdata 
        WHERE pickup_date = '2015-01-03'
        -- WHERE year(pickup_date) = 2015 and month(pickup_date) = 1 and dayOfMonth(pickup_date) = 1;
        """
        
client = get_client()
engine = get_postgres_engine()
    
def main():
    """
    Main function to run the data pipeline modules
    1.-----
    2.------
    
    Parameters: None
    
    Return: None
    """
    
    #extract 
    
    fetch_data(client=client, query=query)
    
    load_csv_to_postgres('tripdata.csv','tripdata', engine, 'STG')
    
    # execute stored procedure
    session = sessionmaker(bind=engine)
    session = session()
    session.execute(text('CALL "STG".agg_tripdata()'))
    session.commit()
    
    print('stored procedure executed')
    
    print('pipeline executed successfully')
    
if __name__ == '__main__': 
    main()