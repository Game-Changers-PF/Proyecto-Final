import os
import pyodbc
import boto3
import pandas as pd
from dotenv import load_dotenv
from io import StringIO
# Manually set environment variables
os.environ['DB_SERVER'] = 'gamechangers-pf.croaauuoulfc.sa-east-1.rds.amazonaws.com,1433'
os.environ['DB_DATABASE'] = 'Game_Changers'
os.environ['DB_USERNAME'] = 'admin'
os.environ['DB_PASSWORD'] = 'Redsar050693'

def connect_to_database():
    try:
        server = os.getenv('DB_SERVER')
        database = os.getenv('DB_DATABASE')
        username = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        driver = '{ODBC Driver 17 for SQL Server}'  # Or ODBC Driver 18
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        
        print(f"Connecting to database with connection string: {connection_string}")
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def read_csv_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    
    # Rename columns to match database
    df.rename(columns={
        'Player ID': 'Player_ID',
        'Full Name': 'Full_Name'
    }, inplace=True)
    
    # Print DataFrame columns for verification
    print(f"DataFrame columns: {df.columns}")
    
    return df


def insert_data_to_database(df, table_name):
    conn = connect_to_database()
    cursor = conn.cursor()

    # Correct column names based on the table schema
    columns = ", ".join([f"[Player_ID]", f"[Full_Name]", f"[Status]"])
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    print(f"Generated SQL query: {insert_query}")

    for index, row in df.iterrows():
        try:
            # Create a tuple with row data corresponding to the SQL placeholders
            data_tuple = (row['Player_ID'], row['Full_Name'], row['Status'])
            print(f"Inserting row: {data_tuple}")  # Debug print for each row
            cursor.execute(insert_query, data_tuple)
        except Exception as e:
            print(f"Error inserting row {data_tuple}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()

def automate_data_loading(bucket_name, key, table_name):
    try:
        df = read_csv_from_s3(bucket_name, key)
        insert_data_to_database(df, table_name)
        return {'statusCode': 200, 'body': 'Data ingested successfully'}
    except Exception as e:
        print(f"Error during data loading: {e}")
        return {'statusCode': 500, 'body': str(e)}

if __name__ == "__main__":
    # Parámetros para la carga de datos
    bucket_name = 'gamechangersnba'
    key = 'csv_filtrados/cleaned_player.csv'  # Especifica la clave correcta del archivo en tu bucket S3
    table_name = 'dbo.all_nba_players_status'  # Cambiado a la tabla correcta

    # Llamar a la función principal
    response = automate_data_loading(bucket_name, key, table_name)
    print(response)
