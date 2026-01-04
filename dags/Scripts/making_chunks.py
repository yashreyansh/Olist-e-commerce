import pandas as pd
import os
import numpy as np

# Configuration
INPUT_PATH = "./raw_data/"  # Folder where your Olist CSVs are
OUTPUT_BASE = "./data_chunks/"
CHUNKS = 24

def create_mismatched_chunks():
    # 1. Load Data
    print("Loading datasets...")
    orders = pd.read_csv(f"{INPUT_PATH}olist_orders_dataset.csv")
    payments = pd.read_csv(f"{INPUT_PATH}olist_order_payments_dataset.csv")

    # 2. Sort orders by time to make chunks chronological
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    orders = orders.sort_values('order_purchase_timestamp')

    # 3. Split orders into 24 chunks
    order_chunks = np.array_split(orders, CHUNKS)
    
    # Ensure directories exist
    os.makedirs(f"{OUTPUT_BASE}orders", exist_ok=True)
    os.makedirs(f"{OUTPUT_BASE}payments", exist_ok=True)

    print(f"Creating {CHUNKS} batches...")

    for i in range(CHUNKS):
        batch_id = i + 1
        
        # Save Order Chunk
        current_orders = order_chunks[i]
        order_file = f"{OUTPUT_BASE}orders/orders_batch_{batch_id}.csv"
        current_orders.to_csv(order_file, index=False)
        
        # --- LATE ARRIVAL LOGIC ---
        # We take the payments for the CURRENT orders, but we save them 
        # in the NEXT batch's file.
        
        # In Batch 1, we save NO payments (simulating a delay).
        # In Batch 2, we save the payments for Batch 1 orders.
        
        if batch_id < CHUNKS:
            next_batch_id = batch_id + 1
            # Get order IDs from the current batch
            current_order_ids = current_orders['order_id'].unique()
            
            # Find matching payments
            matching_payments = payments[payments['order_id'].isin(current_order_ids)]
            
            payment_file = f"{OUTPUT_BASE}payments/payments_batch_{next_batch_id}.csv"
            matching_payments.to_csv(payment_file, index=False)
            
    print(f"Done! Your simulation data is ready in {OUTPUT_BASE}")

if __name__ == "__main__":
    create_mismatched_chunks()