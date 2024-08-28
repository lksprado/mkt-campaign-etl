from sqlalchemy import create_engine,text
import psycopg2
import dotenv
import os 
import pandas as pd 
import time

dotenv.load_dotenv()

DB_HOST=os.getenv('DB_HOST_PROD')
DB_PORT=os.getenv('DB_PORT_PROD')
DB_NAME=os.getenv('DB_NAME_PROD')
DB_USER=os.getenv('DB_USER_PROD')
DB_PASS=os.getenv('DB_PASS_PROD')
DB_SCHEMA=os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def load_to_db(file_path_1: str, table_name_1: str, db_url: str):
    engine = create_engine(db_url)
    try:
        df1 = pd.read_csv(file_path_1, sep=',')
        
        df1.to_sql(table_name_1,con=engine, schema='dw_lcs', if_exists='replace', index=False)
    
    
    except Exception as e:
        print(f"Failed to load data into database:{e}")
    finally:
        engine.dispose()

def transformation(db_url: str):
    engine = create_engine(db_url)
    try:
        query = text(""" 
            CREATE OR REPLACE VIEW mkt_summary AS 
            SELECT
            c.message_type,
            m.channel,
            count(CASE WHEN m.is_opened='t' and m.is_purchased = 't' then m.message_id end) as qtd_compras,
            count(m.message_id) as qtd_messages,
            count(CASE WHEN m.is_opened='t' and m.purchased_at is not null then id end)/count(m.message_id) as conversao_rate
            from messages m
            group by 
            m.message_type,
            m.channel
            """
            )
        with engine.connect() as connection:
            connection.execute(query)
    except Exception as e:
        print(f"Failed to perform transformation: {e}")
    finally:
        engine.dispose()

    
if __name__=="__main__":
    start_time = time.time()
    load_to_db('data/raw/messages-demo.csv', 'mkt_messages', DATABASE_URL)
    transformation(DATABASE_URL)
    end_time = time.time()
    final_time = end_time-start_time
    with open("operation_elt_time.txt", 'w') as file:
        file.write(str(final_time))