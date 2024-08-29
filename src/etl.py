import pandas as pd 
import os
import pyarrow as pa 
import pyarrow.parquet as pq 
import boto3 

def process_csv(csv_file: str, output_dir: str, bucket_name: str, s3_key: str)-> None:
    
    # instanciar o s3
    s3 = boto3.client('s3')
    
    df = pd.read_csv(csv_file)
    
    json_file = os.path.join(output_dir, 'messages-demo.json')
    
    # transformar em json
    df.to_json(json_file, orient='records', lines=False)
    
    parquet_file = os.path.join(output_dir, 'messages-demo.parquet')
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)
    
    s3.upload_file(parquet_file, bucket_name, s3_key)
    