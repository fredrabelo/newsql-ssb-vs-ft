import mysql.connector
import time
import os
import datetime

import pandas as pd

def sql(bd = "singlestore", database = "sf1"):
    # 
    if bd == "singlestore":
        # PROD
        connect_host = "svc-d17f2e29-a88c-4057-bd66-15038cd3a24e-ddl.aws-virginia-2.svc.singlestore.com"
    else:
        # other hosts
        connect_host = "svc-d17f2e29-a88c-4057-bd66-15038cd3a24e-ddl.aws-virginia-2.svc.singlestore.com"

    # 
    return mysql.connector.connect(
        host=connect_host,
        user="admin",
        password="Fredengcomp90$",
        database=database
    )

def select(query, fetch = "all", bd = "singlestore", database = "sf1"):
    # 
    print(query)
    # 
    con = sql(bd, database)
    # 
    if con.is_connected():
        # 
        cursor = con.cursor(buffered=True)
        cursor.execute('DROP ALL FROM PLANCACHE')
        cursor.execute(query)

        # 
        if cursor:
            if fetch == "first":
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()

        # 
        cursor.close()
        con.close()

        # 
        return result
    else:
        # 
        print("Error: not connected")


def timed(query, query_type, query_id, bd = "singlestore", database = "sf1", iteration = 1):
    # 
    con = sql(bd, database)
    # 
    if con.is_connected():
        # 
        cursor = con.cursor(buffered=True)
        cursor.execute('DROP ALL FROM PLANCACHE')
        start_time = time.time()
        cursor.execute(query)
        end_time = time.time()
        elapsed = end_time - start_time
        # 
        res = {"i": iteration, "database": database, "query_type": query_type, "query_id": query_id, "time": elapsed, "query": query}
        #print(res)
        #
        pd_res = pd.DataFrame.from_records([res])
        # 
        cursor.close()
        con.close()

        # 
        return pd_res
    else:
        # 
        print("Error: not connected")

