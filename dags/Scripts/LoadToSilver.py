
from datetime import datetime
import os,sys
import glob
from gamma.Scripts.dependencies.spark import start_spark
from gamma.Scripts.dependencies.archieveFiles import archiveFile
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType, LongType
from pyspark.sql.functions import col, coalesce,to_timestamp,collect_set, concat_ws,sort_array, max, sum,lit, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException


def process_available_files(Job_start_time,**context):

    airflow_run_id = context['run_id']
    
    # will add locgic to check in postgres if its processed or not

    # check file list , if anything exists
    LANDING_ZONE_orders = "/home/DataStuff/data/Olist_e-commerce/Order_chunks"
    LANDING_ZONE_payments = "/home/DataStuff/data/Olist_e-commerce/Payment_chunks"
    SilverLayerPath = "/home/DataStuff/data/Olist_e-commerce/SilverLayer"

    PaymentFactLocation = SilverLayerPath+"/Payment_fact"
    OrderSummaryLocation = SilverLayerPath+"/Order_summary"

    current_time =  datetime.now().strftime("%Y%m%d_%H%M")
    all_files_orders = glob.glob(f"{LANDING_ZONE_orders}/*.parquet")
    all_files_payments = glob.glob(f"{LANDING_ZONE_payments}/*.parquet")
    if not all_files_orders and  not all_files_payments:
        print(f"Checked {LANDING_ZONE_orders} and {LANDING_ZONE_payments} ")
        print("No new files found to process...")
        return    
    #---------------------------------------------#
    # Schema creation
    payment_fact_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", LongType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_value", FloatType(), True),
        StructField("proc_run_id", StringType(), True),
        StructField("created_on", TimestampType(), True),
        StructField("source_payment_file", StringType(), True),
        StructField("updated_on", TimestampType(), True),
        ]
    )
    order_summary_schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
            StructField("no_of_payments", LongType(), True),   # max(payment_sequential)
            StructField("payment_installments", IntegerType(), True),   # max(payment_installments)
            StructField("payment_types_used", StringType(), True),   # listagg(payment_type)
            StructField("total_payment_value", FloatType(), True) ,  # sum(payment_value)
            StructField("created_by_proc_run_id", StringType(), True),
            StructField("updated_by_proc_run_id", StringType(), True),
            StructField("created_on", TimestampType(), True),
            StructField("updated_on", TimestampType(), True),            
        StructField("source_order_file", StringType(), True),
        StructField("source_payment_file", StringType(), True),
            ]
        )   
    timestamp_columns = [
    "order_approved_at",
    "order_purchase_timestamp", 
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "created_on",
    "updated_on"
        ]
# -------------------------------------------#    
    # start spark if any file exists to load
    spark, logger = start_spark()
    try:
        if all_files_orders:
            print(f"Processing Order files...")
            print(all_files_orders)
            order_df = spark.read.parquet(LANDING_ZONE_orders)
            order_df = order_df.withColumn("created_on",current_timestamp() )\
                                .withColumn("updated_on",current_timestamp())\
                                .withColumn("source_order_file",lit(",".join([os.path.basename(f) for f in all_files_orders])) )\
                                .withColumn("proc_run_id",lit(airflow_run_id))\
                                .withColumn("source_payment_file",lit(None).cast(StringType())) \
                                .withColumn("created_by_proc_run_id",lit(None).cast(StringType())) \
                                .withColumn("updated_by_proc_run_id",lit(None).cast(StringType())) 
            for ts_col in timestamp_columns:
                if ts_col in order_df.columns:
                    order_df = order_df.withColumn(ts_col, to_timestamp(col(ts_col)))
        else:
            print("No order files to load...")
            order_df = spark.createDataFrame([],order_summary_schema )
        
        if all_files_payments:
            print(f"Processing Order files...")
            print(all_files_payments)
            payment_df = spark.read.parquet(LANDING_ZONE_payments)
            # casting a column as spark is taking it as binary
            payment_df = payment_df.withColumn("payment_installments", col("payment_installments").cast(IntegerType())) \
                                    .withColumn("proc_run_id",lit(airflow_run_id))\
                                    .withColumn("created_on",current_timestamp())\
                                    .withColumn("source_payment_file" , lit(",".join([os.path.basename(f) for f in all_files_payments])))\
                                    .withColumn("updated_on",current_timestamp())
        else:
            print("No payment files to load...")
            payment_df = spark.createDataFrame([],payment_fact_schema )
    
    #-----------------------------------------------#
        # creating payment fact table
        try:
            payment_fact_table = DeltaTable.forPath(spark,PaymentFactLocation)
            print("Payment_fact table exists, loading...")
        except AnalysisException:
            print("Payment_fact table doesn't exists.... creating")
            spark.createDataFrame([], payment_fact_schema) \
                .write \
                .format("delta") \
                .option("delta.enableChangeDataFeed", "true")\
                .mode("overwrite") \
                .save(PaymentFactLocation)
            payment_fact_table = DeltaTable.forPath(spark, PaymentFactLocation)
        except Exception as e:
            print(e)
            return
        # Order Summary table    
        try:
            order_summary_table = DeltaTable.forPath(spark,OrderSummaryLocation)
            print("Order_summary table exists, loading...")
        except AnalysisException:
            print("Order_summary table doesn't exist, creating...")
            spark.createDataFrame([], order_summary_schema) \
                .write \
                .format("delta") \
                .option("delta.enableChangeDataFeed", "true") \
                .mode("overwrite") \
                .save(OrderSummaryLocation)
            order_summary_table = DeltaTable.forPath(spark, OrderSummaryLocation)
        except Exception as e:
            print(e)
            return
    #-----------------------------------------------------#
    # adding record from new payment files (if any) to payment_fact table
        try:
            payment_fact_table.alias("A") \
                .merge(
                    payment_df.alias("B"),
                    "A.order_id = B.order_id AND A.payment_sequential=B.payment_sequential"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        except Exception as e:
            print("Error while adding records to payment_fact table...")
            print(e)
            return
        
    # taking payment summary to be merged in to order table
        print("Trying to read payment_fact_table")
        payment_fact_data = spark.read.format("delta").load(PaymentFactLocation)
        
        print("Payment_fact_table read successful!!")
        payment_summary = payment_fact_data.groupBy(col("order_id")).agg(
            sum("payment_value").alias("total_payment_value"), 
            max("payment_installments").alias("payment_installments"),
            max("payment_sequential").alias("no_of_payments"),  
            max("updated_on").alias("payment_updated_on"),
            concat_ws(",", sort_array(collect_set("source_payment_file"))).alias("source_payment_file"),
            concat_ws(",", sort_array(collect_set("payment_type"))).alias("payment_types_used")
        )
        print("Successfully aggregated the data from the updated payment fact table....")
        print("Trying to join the record to order file")
        
    # Join payment_summary to Order data from files   
        current_master_order_summary = order_df.alias("o").join(
            payment_summary.alias("p"),
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
                    col("p.no_of_payments"),
                    col("p.payment_installments"),
                    col("p.payment_types_used"),
                    col("p.total_payment_value"),
                    col("o.proc_run_id"),
                    col("o.created_by_proc_run_id"),
                    col("o.updated_by_proc_run_id"),
                    col("o.created_on"),
                    coalesce(col("o.updated_on"), col("p.payment_updated_on")).alias("updated_on"),     
                    col("p.source_payment_file"),
                    col("o.source_order_file")
                )
        print("Updated payment summary has been joined with latest order file...")
        
    # merge to order summary fact table
        print("Merging the orderSummary fact table with the latest summary created...")
        order_summary_table.alias("o") \
            .merge(current_master_order_summary.alias("m"), "o.order_id=m.order_id") \
            .whenMatchedUpdate(set ={
                "order_id": "m.order_id",
                "customer_id": "COALESCE(m.customer_id, o.customer_id)",
                "order_status": "COALESCE(m.order_status, o.order_status)",
                "order_approved_at": "COALESCE(m.order_approved_at, o.order_approved_at)",
                "order_purchase_timestamp": "COALESCE(m.order_purchase_timestamp, o.order_purchase_timestamp)",
                "order_delivered_carrier_date": "COALESCE(m.order_delivered_carrier_date, o.order_delivered_carrier_date)",
                "order_delivered_customer_date": "COALESCE(m.order_delivered_customer_date, o.order_delivered_customer_date)",
                "order_estimated_delivery_date": "COALESCE(m.order_estimated_delivery_date, o.order_estimated_delivery_date)",
                "no_of_payments": "m.no_of_payments",
                "payment_installments": "m.payment_installments",
                "payment_types_used": "m.payment_types_used",
                "total_payment_value": "m.total_payment_value",
                #"created_by_proc_run_id":"",  not required as it must be already created
                "updated_by_proc_run_id": "m.proc_run_id",
                #"created_on":"",   not required as it must be already created
                "updated_on":"m.updated_on",
                "source_payment_file":"COALESCE(o.source_payment_file,m.source_payment_file)",  # to be checked later
                "source_order_file":"COALESCE(o.source_order_file, m.source_order_file)"
            }) \
            .whenNotMatchedInsert(values={
                "order_id": "m.order_id",
                "customer_id": "m.customer_id",
                "order_status": "m.order_status",
                "order_approved_at": "m.order_approved_at",
                "order_purchase_timestamp": "m.order_purchase_timestamp",
                "order_delivered_carrier_date": "m.order_delivered_carrier_date",
                "order_delivered_customer_date": "m.order_delivered_customer_date",
                "order_estimated_delivery_date": "m.order_estimated_delivery_date",
                "no_of_payments": "m.no_of_payments",
                "payment_installments": "m.payment_installments",
                "payment_types_used": "m.payment_types_used",
                "total_payment_value": "m.total_payment_value",
                "created_by_proc_run_id":"m.proc_run_id",  
                "updated_by_proc_run_id": "m.proc_run_id",
                "created_on":"m.created_on",   
                "updated_on":"m.updated_on",
                "source_payment_file":"m.source_payment_file",  # to be checked later
                "source_order_file":"m.source_order_file"
            }
            ) \
            .execute()
        print("merge complete...")
        
        """
        Current Payment File → Merge into Payment Facts (append new payments)
                                    ↓
                            Read ENTIRE Payment Facts table
                                    ↓
                                Aggregate ALL payments per order
                                    ↓
                            Update Order Summary with COMPLETE totals
        """
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
        print(e)
    finally:
        spark.stop()


if __name__ == "__main__":

    process_available_files()