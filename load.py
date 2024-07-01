import pandas as pd

def load_csv_to_postgres(csv_file_path, table_name, engine, schema):
    """
    Loads data from a csv file to a postgres DB table
    
    Parameters:
    -csv_file_path(str): Path to csv file
    -table_name(str): a postgres table
    -engine (sqlalchemy.engine): an SQL alchemy eninge object
    -schema (str): a postgres DB schema
    """
    
    df = pd.read_csv(csv_file_path)
    df.to_sql(table_name, con = engine, if_exists='replace', index=False, schema=schema)
    
    print(f'{len(df)} rows loaded to staging successfully')