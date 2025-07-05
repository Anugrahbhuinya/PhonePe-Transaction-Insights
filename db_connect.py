import mysql.connector
import pandas as pd

def run_query(query, params=None):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Munda403@',   # replace with yours
        database='phonepe'
    )
    if params:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = cursor.column_names
        df = pd.DataFrame(rows, columns=columns)
    else:
        df = pd.read_sql(query, conn)
    conn.close()
    return df
