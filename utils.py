from Bio import Entrez
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import os
from dotenv import load_dotenv

#step1 choper id
#step2 choper docs
#step3 transformer en parquet
#step4 choper metadata
#step5 upload sur S3 bucket

def csvToParquet(path):
    file_name = path.split(',')[0] + ".parquet"
    df = pd.read_csv(path)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_name)


def getMetadata(id="SRX2067140", email="vannson.alexis@gmail.com"):
    Entrez.email = email
    # Fetch the metadata
    handle = Entrez.esearch(db="sra", term=id)
    record = Entrez.read(handle)
    handle.close()

    # Get the ID
    sra_id = record["IdList"][0]

    # Fetch detailed metadata
    handle = Entrez.efetch(db="sra", id=sra_id, rettype="full", retmode="xml")
    metadata = handle.read()
    handle.close()

    return metadata.decode('utf-8')


s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION_NAME', 'eu-north-1')
)

def sendDataToS3Bucket(data, metadata,key='api-data.json', bucket='amarchs3',client=s3_client):
    #metadata is dict
    # Upload to S3
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=str(data),
        Metadata=metadata
    )

