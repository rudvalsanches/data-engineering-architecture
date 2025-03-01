import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

# Configurar Variáveis de ambiente AWS abaixo



# Endpoint da API Open Brewery DB
API_URL = "https://api.openbrewerydb.org/breweries"

def fetch_brewery_data():
    print("Buscando dados da API Open Brewery DB...")
    try:
        resp = requests.get(API_URL)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"Erro ao chamar API. Status: {resp.status_code}")
            return []
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return []

def save_parquet_to_s3(df: pd.DataFrame, file_key: str):
    print("Convertendo DataFrame para Parquet...")
    table = pa.Table.from_pandas(df)
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)

    print("Conectando ao S3 AWS...")
    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Verifica se o bucket existe; se não, cria-o
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except s3_client.exceptions.NoSuchBucket:
        print(f"Bucket {BUCKET_NAME} não existe, criando-o...")
        s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
        )
    except Exception as e:
        print(f"Erro ao verificar bucket: {e}")

    print(f"Fazendo upload do arquivo para s3://{BUCKET_NAME}/{file_key} ...")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    print(f"Arquivo Parquet '{file_key}' salvo no bucket '{BUCKET_NAME}'")

if __name__ == "__main__":
    data = fetch_brewery_data()
    if data:
        df = pd.DataFrame(data)
        # Exemplo: salvando os dados no caminho "breweries_data/breweries.parquet"
        save_parquet_to_s3(df, "breweries_data/breweries.parquet")
    else:
        print("Nenhum dado retornado da API.")
