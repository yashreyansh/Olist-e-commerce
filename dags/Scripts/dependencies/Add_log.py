import psycopg2

def add_log(conn_params, table, user_data):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        query = f"""
        INSERT INTO {table} (RUN_ID, EVENT_TYPE, STATUS)
        VALUES (%s, %s, %s)
        ON CONFLICT (RUN_ID)
        DO UPDATE
            SET 
                EVENT_TYPE=EXCLUDED.EVENT_TYPE,
                STATUS=EXCLUDED.STATUS;
        """
        cur.execute(query , user_data)
        conn.commit()
        print(f"Log added/updated for {user_data[0]}")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

