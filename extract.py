import pandas as pd

# function to get data
def fetch_data(client, query):
    """
    fetches query results from a clickhouse db and writes to a csv file
    
    Paremeters: 
    -click(clickhouse_connect.Client)
    -query (A sql select query)
    
    Returns: None
    """
    #execute the query
    
    output = client.query(query)
    rows = output.result_rows
    output.result_rows
    cols = output.column_names
    
    #close the client connection
    client.close()
    
    df=pd.DataFrame(rows, columns=cols)
    df.to_csv('tripdata.csv', index=False)
    
    print(f'{len(df)} rows successfully extracted')
    
    