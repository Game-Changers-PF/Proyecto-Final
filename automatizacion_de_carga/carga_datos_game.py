import os
import pyodbc
import boto3
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def connect_to_database():
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    driver = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    
    conn = pyodbc.connect(connection_string)
    return conn

def read_csv_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read().decode('utf-8')
    df = pd.read_csv(pd.compat.StringIO(data))
    return df

def insert_data_to_database(df, table_name):
    conn = connect_to_database()
    cursor = conn.cursor()
    
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    for index, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()

def automate_data_loading(bucket_name, key, table_name):
    try:
        df = read_csv_from_s3(bucket_name, key)
        insert_data_to_database(df, table_name)
        return {'statusCode': 200, 'body': 'Data ingested successfully'}
    except Exception as e:
        return {'statusCode': 500, 'body': str(e)}

if __name__ == "__main__":
    # Parámetros para la carga de datos
    bucket_name = 'gamechangersnba'
    key = 'csv_filtrados/cleaned_player_game_stats_2015_2022_cleaned.csv'  # Especifica la clave correcta del archivo en tu bucket S3
    table_name = 'cleaned_game'

    # Llamar a la función principal
    response = automate_data_loading(bucket_name, key, table_name)
    print(response)