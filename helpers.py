from sqlalchemy import create_engine
import clickhouse_connect
from dotenv import load_dotenv
import os

load_dotenv(override=True)

def get_client():
    """
    connects to a clickhouse database using parameters from a .env file
    
    parameters: None
    
    Returns:
    -clickhouse_connect.client: A database clent object
    """
    ## getting credentials
    host = os.getenv('host')
    port = os.getenv('port')
    user = os.getenv('user')
    password = os.getenv('password')
    

   ## connect to database 
    client = clickhouse_connect.get_client(host=host ,port=port,username=user, password=password, secure=True)
    
    return client

def get_postgres_engine():
    """
    Constructs a SQLalchemy engine object for postgres DB from .env file
    
    Paremeters: None
    
    Returns:
    -sqlachemy engine (sqlalchemy.engine.Engine)
    """
    engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
        host = os.getenv('pg_host'),
        port = os.getenv('pg_port'),
        user = os.getenv('pg_user'),
        password = os.getenv('pg_password'),
        dbname = os.getenv('pg_database')
        )
    )
    
    return engine