import pandas as pd
import boto3
from sqlalchemy import create_engine

def export_mysql_to_s3(host, port, user, password, database, table, output_file, bucket_name):
    try:
        # Crear la cadena de conexión para SQLAlchemy
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        # Leer los datos de la tabla en un DataFrame
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        # Exportar a un archivo CSV
        df.to_csv(output_file, index=False)

        # Subir el archivo a S3
        s3_client = boto3.client('s3')
        s3_client.upload_file(output_file, bucket_name, output_file)

        print("Ingesta completada")
    
    except Exception as e:
        print(f"Ocurrió un error: {e}")

# Ejemplo de llamada a la función
export_mysql_to_s3('44.212.63.198', 8005, 'root', 'utec', 'tienda', 'fabricantes', 'data.csv', 'nesc-output-s3')
