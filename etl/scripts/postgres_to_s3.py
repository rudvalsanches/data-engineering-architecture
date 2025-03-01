import os
import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

# Conexão com o PostgreSQL (permanece igual)
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "data_engineering")

# Configurar Variáveis de ambiente AWS abaixo



def read_fictitious_data():
    print("Lendo dados da tabela fictitious_data do PostgreSQL...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    query = "SELECT * FROM fictitious_data;"
    df = pd.read_sql(query, conn)
    print(f"Foram lidas {len(df)} linhas.")
    conn.close()
    return df

def convert_df_to_parquet(df: pd.DataFrame) -> BytesIO:
    print("Convertendo DataFrame para Parquet...")
    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer

def upload_to_s3(buffer: BytesIO, destination_key: str):
    print("Conectando ao S3 AWS...")
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    print(f"Fazendo upload do arquivo para s3://{BUCKET_NAME}/{destination_key} ...")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=destination_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    print("Upload concluído.")

if __name__ == "__main__":
    df = read_fictitious_data()
    if df is not None and not df.empty:
        parquet_buffer = convert_df_to_parquet(df)
        # Salvar em uma pasta separada, por exemplo: "postgres_data/fictitious_data.parquet"
        destination_key = "postgres_data/fictitious_data.parquet"
        upload_to_s3(parquet_buffer, destination_key)
    else:
        print("Nenhum dado encontrado na tabela fictitious_data.")
