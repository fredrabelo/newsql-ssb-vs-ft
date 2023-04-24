import psycopg2
from psycopg2.errors import SerializationFailure
import psycopg2.extras
import pandas as pd
import numpy as np
import time
from scipy.stats import t, variation
from datetime import datetime
import os

def ci(data, cf=0.95):
    m = np.mean(data)
    s = np.std(data)
    l = len(data)
    t_crit = np.abs(t.ppf((1-cf)/2,l-1))
    return m, m-s*t_crit/np.sqrt(l), m+s*t_crit/np.sqrt(l)


base_query = "SELECT metadata ->> 'query' AS statement_text, statistics->'statistics'->'svcLat'->'mean' AS TimeElapsed_mean, statistics->'statistics'->'svcLat'->'sqDiff' AS TimeElapsed_sqDiff, statistics->'statistics'->'bytesRead'->'mean' AS BytesRead_mean, statistics->'statistics'->'bytesRead'->'sqDiff' AS BytesRead_sqDiff, statistics -> 'execution_statistics' -> 'maxMemUsage' -> 'mean' AS Memory_mean, statistics -> 'execution_statistics' -> 'maxMemUsage' -> 'sqDiff' AS Memory_sqDiff FROM crdb_internal.statement_statistics where metadata ->> 'query' like '%"
base_query2 = "%' order by aggregated_ts desc;"


queries_ft = [
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder_obt where d_order_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder_obt where d_order_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;",
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder_obt where d_order_weeknuminyear = 6 and d_order_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    "select sum(lo_revenue), d_order_year, p_brand from lineorder_obt where p_category = 'MFGR#12' and s_region = 'AMERICA' group by d_order_year, p_brand order by d_order_year, p_brand;",
    "select sum(lo_revenue), d_order_year, p_brand from lineorder_obt where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_order_year, p_brand order by d_order_year, p_brand;",
    "select sum(lo_revenue), d_order_year, p_brand from lineorder_obt where p_brand = 'MFGR#2221' and s_region = 'EUROPE' group by d_order_year, p_brand order by d_order_year, p_brand;",
    "select c_nation, s_nation, d_order_year, sum(lo_revenue) as revenue from lineorder_obt where c_region = 'ASIA' and s_region = 'ASIA' and d_order_year >= 1992 and d_order_year <= 1997 group by c_nation, s_nation, d_order_year order by d_order_year asc, revenue desc;",
    "select c_city, s_city, d_order_year, sum(lo_revenue) as revenue from lineorder_obt where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_order_year >= 1992 and d_order_year <= 1997 group by c_city, s_city, d_order_year order by d_order_year asc, revenue desc;",
    "select c_city, s_city, d_order_year, sum(lo_revenue) as revenue from lineorder_obt where d_order_year >= 1992 and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_order_year <= 1997 group by c_city, s_city, d_order_year order by d_order_year asc, revenue desc;",
    "select c_city, s_city, d_order_year, sum(lo_revenue) as revenue from lineorder_obt where d_order_yearmonth = 'Dec1997' and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') group by c_city, s_city, d_order_year order by d_order_year asc, revenue desc;",
    "select d_order_year, c_nation, sum(lo_revenue - lo_supplycost) as profit from lineorder_obt where c_region = 'AMERICA'  and s_region = 'AMERICA'  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_order_year, c_nation order by d_order_year, c_nation;",
    "select d_order_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit from lineorder_obt where c_region = 'AMERICA' and s_region = 'AMERICA' and (d_order_year = 1997 or d_order_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_order_year, s_nation, p_category order by d_order_year, s_nation, p_category;",
    "select d_order_year, s_city, p_brand, sum(lo_revenue - lo_supplycost) as profit from lineorder_obt where c_region = 'AMERICA' and s_nation = 'UNITED STATES' and (d_order_year = 1997 or d_order_year = 1998) and p_category = 'MFGR#14' group by d_order_year, s_city, p_brand order by d_order_year, s_city, p_brand;"]


queries_text_ft = [
    "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder_obt WHERE ((d_order_yearmonthnum",
    "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder_obt WHERE ((d_order_year",
    "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder_obt WHERE (((d_order_weeknuminyear",
    "SELECT sum(lo_revenue), d_order_year, p_brand FROM lineorder_obt WHERE (p_category",
    "SELECT sum(lo_revenue), d_order_year, p_brand FROM lineorder_obt WHERE (p_brand BETWEEN",
    "SELECT sum(lo_revenue), d_order_year, p_brand FROM lineorder_obt WHERE (p_brand =",
    "SELECT c_nation",
    "SELECT c_city, s_city, d_order_year, sum(lo_revenue) AS revenue FROM lineorder_obt WHERE (((c_nation",
    "SELECT c_city, s_city, d_order_year, sum(lo_revenue) AS revenue FROM lineorder_obt WHERE (((d_order_year",
    "SELECT c_city, s_city, d_order_year, sum(lo_revenue) AS revenue FROM lineorder_obt WHERE ((d_order_yearmonth",
    "SELECT d_order_year, c_nation",
    "SELECT d_order_year, s_nation",
    "SELECT d_order_year, s_city"
]


host = "HOST.aws-us-east-1.cockroachlabs.cloud"  #sf10 
user = "fredrabelo"
password = "XXXXXXXXXXXXXXXX" #sf10 
datatype = "flat"
database = "sf10-flat"
size = "sf10"
n_query = 60
retry_count = 0
conn = psycopg2.connect(host=host, user=user,password=password, dbname=database, port=26257)
if conn:
    try:
        print('Connected')
        for i in [11]:
            exec = []
            start = datetime.today()
            print("start_{}:{}".format(i, start.strftime("%Y-%m-%d %H:%M:%S")))
            for _ in range(0, n_query+1):
                go = True
                while go and retry_count < 1:
                    cursor = conn.cursor()
                    cursor.execute(queries_ft[i])
                    cursor.close()
                    conn.close()
                    conn = psycopg2.connect(host=host, user=user,password=password, dbname=database, port=26257)
                    cursor = conn.cursor()
                    cursor.execute(base_query + queries_text_ft[i] + base_query2)
                    res_fetchall = cursor.fetchall()
                    print(res_fetchall[0][1])
                    try:
                        res = {"ELAPSED_TIME": res_fetchall[0][1], "BYTES_READ": res_fetchall[0][3], "MEAN_MEMORY": res_fetchall[0][5]}
                        pd_res = pd.DataFrame.from_records([res], coerce_float=True)
                        exec.append(pd_res)
                    except:
                        pass
                    else:
                        go = False
                    finally:
                        cursor.close()
                    #print('3')
            results = pd.concat(exec)
            results.to_csv("data/{}/{}/{}.csv".format(size,datatype,i), index=False, header=True)
            analysis = {"mean": [], "mean_min": [], "mean_max": [], "std": [], "var": [], "cv": [], "min": [], "max": [], "median": []}
            atts = ["ELAPSED_TIME", "BYTES_READ", "MEAN_MEMORY"]
            for att in atts:
                res = ci(results[att])
                analysis["mean"].append(res[0])
                analysis["mean_min"].append(res[1])
                analysis["mean_max"].append(res[2])
                analysis["std"].append(np.std(results[att]))
                analysis["var"].append(np.var(results[att]))
                analysis["cv"].append(variation(results[att]))
                analysis["min"].append(np.min(results[att]))
                analysis["max"].append(np.max(results[att]))
                analysis["median"].append(np.median(results[att]))
            analysis = pd.DataFrame.from_dict(analysis, orient='index', columns=atts)
            analysis.to_csv("data/{}/{}/analysis_{}.csv".format(size,datatype,i), index=True, header=True)
            end = datetime.today()
            print("end_{}:{}".format(i, end.strftime("%Y-%m-%d %H:%M:%S")))
            delta = str(end - start).split(".")[0]
            times = {"START": start.strftime("%Y-%m-%d %H:%M:%S"), "FINISH": end.strftime("%Y-%m-%d %H:%M:%S"), "DELTA": delta}
            pd.DataFrame.from_dict([times]).to_csv("data/{}/{}/times.csv".format(size,datatype), mode="a", index=False, header=not os.path.exists(f"C:/Users/fredrabelo/Desktop/Doutorado/Experimentos/CockroachDB/data/{size}/{datatype}/times.csv"))
    except Exception as e:
        print(repr(e))
        print ("Retry after 5 sec")
        retry_count = retry_count + 1
        cursor.close()
        conn.close()
        time.sleep(5)
        conn = psycopg2.connect(host=host, user=user,password=password, dbname=database, port=26257)
        cursor = conn.cursor()
    finally:
        conn.close()
else:
    print("Error: not connected")