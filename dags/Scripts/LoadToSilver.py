
from datetime import datetime
import os
import glob
from dependencies.spark import start_spark
from dependencies.archieveFiles import archiveFile
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType, LongType
from pyspark.sql.functions import col, coalesce,to_timestamp
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException


def process_available_files():
    master_schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
            StructField("payment_sequential", LongType(), True),
            StructField("payment_installments", IntegerType(), True),
            StructField("payment_type", StringType(), True),
            StructField("payment_value", FloatType(), True)
            ]
        )
    # will add locgic to check in postgres if its processed or not

    # check file list , if anything exists
    LANDING_ZONE_orders = "/home/DataStuff/data/Olist_e-commerce/Order_chunks"
    LANDING_ZONE_payments = "/home/DataStuff/data/Olist_e-commerce/Payment_chunks"
    SilverLayerPath = "/home/DataStuff/data/Olist_e-commerce/SilverLayer"

    all_files_orders = glob.glob(f"{LANDING_ZONE_orders}/*.parquet")
    all_files_payments = glob.glob(f"{LANDING_ZONE_payments}/*.parquet")
    if not all_files_orders and  not all_files_payments:
        print(f"Checked {LANDING_ZONE_orders} and {LANDING_ZONE_payments} ")
        print("No new files found to process...")
        return    
# -------------------------------------------#    
    # start spark if any file exists to load
    spark, logger = start_spark()
    try:
        if all_files_orders:
            print(f"Processing Order files...")
            print(all_files_orders)
            order_df = spark.read.parquet(LANDING_ZONE_orders)
            for ts_col in ['c']:
                if ts_col in order_df.columns:
                    order_df = order_df.withColumn(ts_col, to_timestamp(col(ts_col)))
        else:
            print("No order files to load...")
            order_df = spark.createDataFrame([],master_schema )
        
        if all_files_payments:
            print(f" Processing Order files...")
            print(all_files_payments)
            payment_df = spark.read.parquet(LANDING_ZONE_payments) 
            # casting a column as spark is taking it as binary
            if "payment_installments" in payment_df.columns:
                payment_df = payment_df.withColumn("payment_installments", col("payment_installments").cast(IntegerType()))
        else:
            print("No payment files to load...")
            payment_df = spark.createDataFrame([],master_schema )
    #---------------------------------------------#
        master_df = order_df.alias("o").join(
            payment_df.alias("p"), 
            on='order_id',
            how='outer'
            ).select(
                coalesce(col("o.order_id"), col("p.order_id")).alias("order_id"),
                col("o.customer_id"),
                col("o.order_status"),
                col("o.order_approved_at"),
                col("o.order_purchase_timestamp"),
                col("o.order_delivered_carrier_date"),
                col("o.order_delivered_customer_date"),
                col("o.order_estimated_delivery_date"),
                col("p.payment_sequential"),
                col("p.payment_installments"),
                col("p.payment_type"),
                col("p.payment_value")
                    )  
        
        #print(f"MasterSchmema I am using : {master_df.printSchema()}")
        try:
            delta_table = DeltaTable.forPath(spark,SilverLayerPath)
            print("Delta table exists, loading...")
        except AnalysisException:
            print("Delta table doesn't exist, creating...")
            spark.createDataFrame([], master_schema) \
                .write \
                .format("delta") \
                .mode("overwrite") \
                .save(SilverLayerPath)
            delta_table = DeltaTable.forPath(spark, SilverLayerPath)
        except Exception as e:
            print(e)
            return
        print("Moving data to delta tables..")

        # S for delta table record, C for the current processing file record
        # if a record exists partially, below will update with relevant values
        try:
            delta_table.alias("S")\
                .merge(
                    master_df.alias("C"),
                    "S.order_id = C.order_id"
                ) \
                .whenMatchedUpdate(
                    set = {
                "order_id": "COALESCE(C.order_id,S.customer_id)",
                "customer_id": "COALESCE(C.customer_id, S.customer_id)",
                "order_status": "COALESCE(C.order_status, S.order_status)",
                "order_approved_at": "COALESCE(C.order_approved_at, S.order_approved_at)",
                "order_purchase_timestamp": "COALESCE(C.order_purchase_timestamp, S.order_purchase_timestamp)",
                "order_delivered_carrier_date": "COALESCE(C.order_delivered_carrier_date, S.order_delivered_carrier_date)",
                "order_delivered_customer_date": "COALESCE(C.order_delivered_customer_date, S.order_delivered_customer_date)",
                "order_estimated_delivery_date": "COALESCE(C.order_estimated_delivery_date, S.order_estimated_delivery_date)",
                "payment_sequential": "COALESCE(C.payment_sequential, S.payment_sequential)",
                "payment_installments": "COALESCE(C.payment_installments, S.payment_installments)",
                "payment_type": "COALESCE(C.payment_type, S.payment_type)",
                "payment_value": "COALESCE(C.payment_value, S.payment_value)",
                }
                    
                ) \
                .whenNotMatchedInsertAll() \
                .execute()
        except Exception as e:
            print("Error in savings in delta table")
            print(e)
            print("Aborting the process for now...")    
            return     

        '''
        master_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema",True) \
            .save(SilverLayerPath)
        '''



        print("Succesfully moved to delta table")

    #----------------------------------------------#
        # 3. After successful merge, move the file to a 'processed' folder
        archive_order_dir = f"{LANDING_ZONE_orders}/processed/"
        archive_payment_dir = f"{LANDING_ZONE_payments}/processed/"
        try:
            archiveFile(LANDING_ZONE_orders ,archive_order_dir , '*.parquet' )
            print("Moved Order files to processed folder (if any)")
            archiveFile(LANDING_ZONE_payments ,archive_payment_dir , '*.parquet' )
            print("Moved payment files to processed folder (if any)")
        except Exception as e:
            print("Issue in archiving orders files")
            print(e)
        print(f" Finished order files and moved to archive")


    except Exception as e:
        print("Something wrong happened!!!!")
    finally:
        spark.stop()


if __name__ == "__main__":

    process_available_files()