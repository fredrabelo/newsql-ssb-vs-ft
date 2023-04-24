import mariadb
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

queries_ft = [
    "select sum(lo_extendedprice*lo_discount) as revenue from sf10.lineorder_obt where d_order_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
    "select sum(lo_extendedprice*lo_discount) as revenue from sf10.lineorder_obt where d_order_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;",
    "select sum(lo_extendedprice*lo_discount) as revenue from sf10.lineorder_obt where d_order_weeknuminyear = 6 and d_order_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    "select sum(lo_revenue), d_order_year, p_brand from sf10.lineorder_obt where p_category = 'MFGR#12' and s_region = 'AMERICA' group by d_order_year, p_brand order by d_order_year, p_brand;",
    "select sum(lo_revenue), d_order_year, p_brand from sf10.lineorder_obt where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_order_year, p_brand order by d_order_year, p_brand;",
    "select sum(lo_revenue), d_order_year, p_brand from sf10.lineorder_obt where p_brand = 'MFGR#2221' and s_region = 'EUROPE' group by d_order_year, p_brand order by d_order_year, p_brand;",
    "select c_nation, s_nation, d_order_year, sum(lo_revenue) as revenue from sf10.lineorder_obt where c_region = 'ASIA' and s_region = 'ASIA' and d_order_year >= 1992 and d_order_year <= 1997 group by c_nation, s_nation, d_order_year order by d_order_year asc, revenue desc;",
    "select c_city, s_city, d_order_year, sum(lo_revenue) as revenue from sf10.lineorder_obt where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_order_year >= 1992 and d_order_year <= 1997 group by c_city, s_city, d_order_year order by d_order_year asc, revenue desc;",
    "select c_city, s_city, d_order_year, sum(lo_revenue) as revenue from sf10.lineorder_obt where d_order_year >= 1992 and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_order_year <= 1997 group by c_city, s_city, d_order_year order by d_order_year asc, revenue desc;",
    "select c_city, s_city, d_order_year, sum(lo_revenue) as revenue from sf10.lineorder_obt where d_order_yearmonth = 'Dec1997' and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') group by c_city, s_city, d_order_year order by d_order_year asc, revenue desc;",
    "select d_order_year, c_nation, sum(lo_revenue - lo_supplycost) as profit from sf10.lineorder_obt where c_region = 'AMERICA'  and s_region = 'AMERICA'  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_order_year, c_nation order by d_order_year, c_nation;",
    "select d_order_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit from sf10.lineorder_obt where c_region = 'AMERICA' and s_region = 'AMERICA' and (d_order_year = 1997 or d_order_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_order_year, s_nation, p_category order by d_order_year, s_nation, p_category;",
    "select d_order_year, s_city, p_brand, sum(lo_revenue - lo_supplycost) as profit from sf10.lineorder_obt where c_region = 'AMERICA' and s_nation = 'UNITED STATES' and (d_order_year = 1997 or d_order_year = 1998) and p_category = 'MFGR#14' group by d_order_year, s_city, p_brand order by d_order_year, s_city, p_brand;"]

datatype = "flat"
database = "sf10"
size = "sf10"
n_query = 60
retry_count = 0
try:
    print('begin')
    for i in range(0,13):
        conn = mariadb.connect( host="HOST.mdb0002418.db1.skysql.net", port=5001, ssl_ca="/home/fredrabelo/Desktop/Doutorado/Experiment/MariaDB/aws_skysql_chain.pem", user="DB00010242", password="XXXXXXXXXXX")
        print('Connected for query '+str(i))
        cursor = conn.cursor(buffered=True)
        cursor.execute('SET profiling = 1;')
        exec = []
        start = datetime.today()
        print("start_{}:{}".format(i, start.strftime("%Y-%m-%d %H:%M:%S")))
        for j in range(0, n_query+1):
            go = True
            while go and retry_count < 5:
                duration = 0
                cpu_user = 0
                cpu_system = 0
                cursor = conn.cursor(buffered=True)
                cursor.execute(queries_ft[i])
                cursor.execute("SHOW PROFILE CPU, MEMORY, SWAPS FOR QUERY "+str(j)+";")
                res_fetchall = cursor.fetchall()
                for r in res_fetchall: 
                    duration = duration + float(r[1])
                    cpu_user = cpu_user + float(r[2])
                    cpu_system = cpu_system + float(r[3])
                try:
                    res = {"DURATION": duration, "CPU_USER": cpu_user, "CPU_SYSTEM": cpu_system}
                    pd_res = pd.DataFrame.from_records([res], coerce_float=True)
                    exec.append(pd_res)
                except:
                    pass
                else:
                    go = False
                finally:
                    cursor.close()
        conn.close()
        results = pd.concat(exec)
        results.to_csv("data/{}/{}/{}.csv".format(size,datatype,i), index=False, header=True)
        results = results.iloc[1: , :] # eliminando a primeira linha (valor que eh sempre muito alto) para calcular as metricas
        analysis = {"mean": [], "mean_min": [], "mean_max": [], "std": [], "var": [], "cv": [], "min": [], "max": [], "median": []}
        atts = ["DURATION", "CPU_USER", "CPU_SYSTEM"]
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
        pd.DataFrame.from_dict([times]).to_csv("data/{}/{}/times.csv".format(size,datatype), mode="a", index=False, header=not os.path.exists(f"~/Desktop/Doutorado/Experiment/MariaDB/data/sf10/{size}/{datatype}/times.csv"))
except Exception as e:
    print(repr(e))