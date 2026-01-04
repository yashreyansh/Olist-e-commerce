from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

def send_data(chunk, location):
    try:
        df = pd.read_csv(chunk)
        count_ = len(df)
        
        print(f"No of records: {count_}")
        try:
            df.to_parquet(location, engine="pyarrow")
            print(f"Data written to {location}")
        except Exception as e:
            print(f"Couldn't write file to {location}")      
    except Exception as e:
        print(f"File {chunk} does not exists...")


def simulate_chunk(**ctx):
    current_val = int(Variable.get("chunk_index", default_var=1))  
    idx = ((current_val - 1) % 24) + 1    # to loop again to 24
    print(f"Processing chunk {idx}")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    order_chunks = f'/Project_Olist/data_chunks/orders/orders_batch_{idx}.csv'
    payment_chunks = f'/Project_Olist/data_chunks/payments/payments_batch_{idx}.csv'


    target_order_loc = f'/home/DataStuff/data/Olist_e-commerce/Order_chunks'
    target_payment_loc = f'/home/DataStuff/data/Olist_e-commerce/Payment_chunks'
    if not os.path.exists(target_order_loc):
        print(f"Creating directory: {target_order_loc}")
        os.makedirs(target_order_loc, exist_ok=True)
    if not os.path.exists(target_payment_loc):
        print(f"Creating directory: {target_payment_loc}")
        os.makedirs(target_payment_loc, exist_ok=True)


    target_order_file = f'/home/DataStuff/data/Olist_e-commerce/Order_chunks/{idx}.parquet'
    target_payment_file = f'/home/DataStuff/data/Olist_e-commerce/Payment_chunks/{idx}.parquet'
    send_data(payment_chunks,target_payment_file)
    print(f"Payment chunk {idx} sent...")
    
    send_data(order_chunks,target_order_file)
    print(f"Order chunk {idx} sent...")

    # simulate using chunk idx here...
    Variable.set("chunk_index", idx + 1)
    print(f"Chunk val increased to {idx + 1}")

if __name__ == "__main__":
    simulate_chunk()    