import psycopg2


def audit_log(conn, table, RUN_ID, EVENT_TYPE, STATUS, RESP_MESSAGE):
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""INSERT INTO {table} (RUN_ID,EVENT_TYPE,STATUS,RESP_MESSAGE)
            VALUES (%s,%s,%s,%s)
            """, (RUN_ID, EVENT_TYPE, STATUS, RESP_MESSAGE)
        )
        conn.commit()
        cursor.close()
        print(f"Audit updated....with status -> {STATUS}")
    except Exception as e:
        print(f"Couldn't update audit!!!!!!!!!!  .. Database error: {e}")
    '''
    finally:
        if cur: cur.close()
        if conn: conn.close()
    '''
