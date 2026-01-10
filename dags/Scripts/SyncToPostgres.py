# gamma/Scripts/SyncToPostgres.py

import psycopg2
from psycopg2 import sql
from pyspark.sql.functions import col, max as spark_max
import uuid
import traceback
from datetime import datetime
from gamma.Scripts.dependencies.spark import start_spark
from gamma.Scripts.dependencies.Add_log import audit_log


class DeltaToPostgresSync:
    """Sync Delta Lake tables to Postgres using Change Data Feed"""
    
    def __init__(self, postgres_config):
        
        #Initialize with Postgres connection config
        '''
            Args:
            postgres_config: dict with keys: host, database, user, password, port
        '''
        self.audit_table = "OLIST.audit_log"
        self.postgres_config = postgres_config
        self.jdbc_url = f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
        self.spark = None
        self.logger = None
        self.run_id= str(uuid.uuid4())
        
    def start_spark_session(self, conn):
        """Start Spark session"""
        self.spark, self.logger = start_spark()
        audit_log(conn, self.audit_table,self.run_id,"Spark_started", "Started", "Stage 1, spark started...")
        print("Spark session started successfully")
        
    def stop_spark_session(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("Spark session stopped")
    
    def get_postgres_connection(self):
        """Create Postgres connection"""
        return psycopg2.connect(
            host=self.postgres_config['host'],
            database=self.postgres_config['database'],
            user=self.postgres_config['user'],
            password=self.postgres_config['password'],
            port=self.postgres_config['port']
        )
    
    def get_last_synced_version(self, table_name, conn):
        """
        Get the last synced version for a table
        
        Args:
            table_name: Name of the table
            conn: Postgres connection
            
        Returns:
            int: Last synced version, or 0 if never synced
        """
        cursor = conn.cursor()
        cursor.execute(
            "SELECT last_version FROM OLIST.sync_tracking WHERE table_name = %s",
            (table_name,)
        )
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            print(f"Last synced version for {table_name}: {result[0]}")
            return result[0]
        else:
            print(f"No sync history found for {table_name}, starting from version 0")
            return 0
    
    def update_sync_tracking(self, table_name, current_version, run_id, conn):
        """
        Update sync tracking table
        
        Args:
            table_name: Name of the table
            current_version: Current version synced
            run_id: Run ID for this sync
            conn: Postgres connection
        """
        audit_log(conn, self.audit_table,self.run_id,"Sync tracking started", "In-Progress", "Sync tracking for {table_name}")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO OLIST.sync_tracking (table_name, last_version, last_sync_time, run_id)
            VALUES (%s, %s, NOW(), %s)
            ON CONFLICT (table_name)
            DO UPDATE SET 
                last_version = EXCLUDED.last_version,
                last_sync_time = EXCLUDED.last_sync_time,
                run_id = EXCLUDED.run_id
        """, (table_name, current_version, run_id))
        conn.commit()
        cursor.close()

        audit_log(conn, self.audit_table,self.run_id,"Sync tracking completed", "In-Progress", "Sync tracking for {table_name}")
        print(f"Updated sync tracking: {table_name} to version {current_version}")
    
    def sync_table(self,conn, delta_path, postgres_table, staging_table, primary_keys, columns):
        """
        Sync a Delta table to Postgres
        
        Args:
            delta_path: Path to Delta table
            postgres_table: Target Postgres table name
            staging_table: Staging table name
            primary_keys: List of primary key columns (for composite keys)
            columns: List of columns to sync
        """
        run_id = str(uuid.uuid4())
        print(f"\n{'='*80}")
        print(f"Starting sync for {postgres_table}")
        print(f"Run ID: {run_id}")
        print(f"Delta Path: {delta_path}")
        print(f"{'='*80}\n")
        
        #conn = self.get_postgres_connection()
        
        try:
            # 1. Get last synced version
            last_version = self.get_last_synced_version(postgres_table, conn)
            
            # get current delta version
            from delta.tables import DeltaTable
            delta_table_obj = DeltaTable.forPath(self.spark, delta_path)
            lastest_version = delta_table_obj.history(1).select("version").collect()[0]["version"]

            print(f"Last synced version is {last_version}")
            print(f"Current Delta version is {lastest_version}")

            # checking if already up to date
            if last_version>=lastest_version:
                print(f"{postgres_table} is already up to date!")
                #  conn.close()   # now closing in the end of script run
                audit_log(conn, self.audit_table,self.run_id,"Sync not required", "In-Progress", f"{postgres_table} is already synced..")
                return

            # 2. Read changes from Delta using CDF
            print(f"Reading changes from Delta table (version > {last_version})...")
            try:
                changes = self.spark.read.format("delta") \
                    .option("readChangeFeed", "true") \
                    .option("startingVersion", last_version + 1) \
                    .load(delta_path)
                
                # Filter for inserts and updates only
                final_changes = changes.filter(
                    col("_change_type").isin(["insert", "update_postimage"])
                )
                
                change_count = final_changes.count()
                
                if change_count == 0:
                    print(f"No changes to sync for {postgres_table}")
                    #  conn.close()   # now closing in the end of script run
                    audit_log(conn, self.audit_table,self.run_id,"Sync not required", "In-Progress", f"nothing to sync in {postgres_table}")
                    return
                
                print(f"Found {change_count} changes to sync")
                
                # Get current version
                current_version = final_changes.agg(
                    spark_max("_commit_version")
                ).collect()[0][0]
                
                print(f"Current Delta version: {current_version}")
                
            except Exception as e:
                error_msg = str(e)
                if "is not enabled" in error_msg.lower() or "Invalid startingVersion" in error_msg.lower():
                    print(f"Change Data Feed not enabled or no changes. Error: {e}")
                    print("Skipping this table...")
                    audit_log(conn, self.audit_table,self.run_id,"Sync skipped", "In-Progress", "Sync Skipped {postgres_table}, Change Data Feed not enabled or no changes")
                    #  conn.close()   # now closing in the end of script run
                    return
                if "invalid" in error_msg.lower() or "cannot be greater" in error_msg.lower():
                    print(f"Already up to date (version check)")
                    audit_log(conn, self.audit_table,self.run_id,"Sync skipped", "In-Progress", "Sync not required {postgres_table}]")
                    #  conn.close()   # now closing in the end of script run
                    return
                else:
                    raise
            
            # 3. Select only Postgres columns (drop CDF system columns)
            print("Preparing data for Postgres...")
            data_to_sync = final_changes.select(columns)
            
            # Show sample data
            print("\nSample data to sync:")
            data_to_sync.show(5, truncate=False)
            
            # 4. Write to Postgres staging table
            print(f"Writing {change_count} records to staging table: {staging_table}")
            data_to_sync.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", staging_table) \
                .option("user", self.postgres_config['user']) \
                .option("password", self.postgres_config['password']) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            audit_log(conn, self.audit_table,self.run_id,"Sync completed", "In-Progress", "Sync completed for {postgres_table}]")
            print("Write to staging complete!")
            
            # 5. Execute UPSERT in Postgres
            print(f"Executing UPSERT from {staging_table} to {postgres_table}...")
            self.execute_upsert(postgres_table, staging_table, primary_keys, columns, conn)
            
            # 6. Update sync tracking
            self.update_sync_tracking(postgres_table, current_version, run_id, conn)
            
            print(f"\n✅ Successfully synced {postgres_table}!")
            print(f"   Records synced: {change_count}")
            print(f"   Version: {last_version} → {current_version}")
            print(f"   Run ID: {run_id}\n")
            
        except Exception as e:
            print(f"\n❌ Error syncing {postgres_table}: {e}")
            traceback.print_exc()
            conn.rollback()
            audit_log(conn, self.audit_table,self.run_id,"Error while syncing table", "In-Progress", f"sync_table -> {postgres_table}: {e}")
            raise
        finally:
            #  conn.close()   # now closing in the end of script run
            pass
    
    def execute_upsert(self, target_table, staging_table, primary_keys, columns, conn):
        """
        Execute UPSERT from staging to target table
        
        Args:
            target_table: Target table name
            staging_table: Staging table name
            primary_keys: List of primary key columns
            columns: List of all columns
            conn: Postgres connection
        """
        audit_log(conn, self.audit_table,self.run_id,"Upsert started to taget table", "In-Progress", "Upsert started for  {target_table}]")
        cursor = conn.cursor()
        
        # Build column lists
        columns_str = ", ".join(columns)
        
        # Build UPDATE SET clause (exclude primary keys from update)
        update_columns = [col for col in columns if col not in primary_keys]
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        # Build conflict target (can be single or composite key)
        conflict_target = ", ".join(primary_keys)
        
        # Build UPSERT query
        upsert_query = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT {columns_str} FROM {staging_table}
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_set};
        """
        
        print("Executing UPSERT query...")
        cursor.execute(upsert_query)
        rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()

        audit_log(conn, self.audit_table,self.run_id,"Upsert started to taget table", "In-Progress", "Upsert completed for  {target_table}]")

        print(f"UPSERT complete! Rows affected: {rows_affected}")


def sync_payment_facts(syncer,conn):
    """Sync Payment Facts table"""
    audit_log(conn, syncer.audit_table,syncer.run_id,"Syncing_Payment_facts", "In-Progress", "syncing payment fact")

    syncer.sync_table(conn,
        delta_path="/home/DataStuff/data/Olist_e-commerce/SilverLayer/Payment_fact",
        postgres_table="OLIST.payment_facts",
        staging_table="OLIST.payment_facts_staging",
        primary_keys=["order_id", "payment_sequential"],  # Composite key
        columns=[
            "order_id",
            "payment_sequential",
            "payment_installments",
            "payment_type",
            "payment_value",
            "proc_run_id",
            "source_payment_file",
            "created_on",
            "updated_on"
        ]
    )


def sync_order_summary(syncer,conn):
    """Sync Order Summary table"""
    audit_log(conn, syncer.audit_table,syncer.run_id,"Syncing_Payment_facts", "In-Progress", "syncing payment fact")

    syncer.sync_table(conn,
        delta_path="/home/DataStuff/data/Olist_e-commerce/SilverLayer/Order_summary",
        postgres_table="OLIST.order_summary",
        staging_table="OLIST.order_summary_staging",
        primary_keys=["order_id"],  # Single key
        columns=[
            "order_id",
            "customer_id",
            "order_status",
            "order_approved_at",
            "order_purchase_timestamp",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "no_of_payments",
            "payment_installments",
            "payment_types_used",
            "total_payment_value",
            "created_by_proc_run_id",
            "updated_by_proc_run_id",
            "source_order_file",
            "source_payment_file",
            "created_on",
            "updated_on"
        ]
    )


def main():
    """Main sync function"""
    
    # Postgres configuration
    postgres_config = {
        'host': 'postgres',
        'port': 5432,
        'database': 'data_db',
        'user': 'admin',
        'password': 'admin' 
    }
    
    print("\n" + "="*80)
    print("STARTING DELTA TO POSTGRES SYNC")
    print("="*80 + "\n")
    
    # Initialize syncer
    syncer = DeltaToPostgresSync(postgres_config)
    conn = syncer.get_postgres_connection()
    try:
        # Start Spark
        syncer.start_spark_session(conn)
        
        # Sync tables
        print("\n1. Syncing Payment Facts...")
        sync_payment_facts(syncer,conn)
        
        print("\n2. Syncing Order Summary...")
        sync_order_summary(syncer,conn)
        
        print("\n" + "="*80)
        print("✅ ALL SYNCS COMPLETE!")
        print("="*80 + "\n")

        audit_log(conn, syncer.audit_table,syncer.run_id,"All sync completed", "Completed", "All done!!!")

    except Exception as e:
        print(f"\n❌ SYNC FAILED: {e}")
        traceback.print_exc()
        audit_log(conn, syncer.audit_table,syncer.run_id,"Error happened..", "Failed", f"{e}")
        raise
    finally:
        # Stop Spark
        conn.close()
        syncer.stop_spark_session()


if __name__ == "__main__":
    main() 