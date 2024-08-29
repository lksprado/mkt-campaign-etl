from src.etl import process_csv

def main():
    csv_file = 'data/raw/messages-demo.csv'
    output_dir = 'data/processed'
    bucket_name = 'mkt-campaings'
    s3_key = 'parquet_files/messages-demo.parquet'
    
    process_csv(csv_file,output_dir, bucket_name, s3_key)
    
if __name__ == "__main__":
    main()