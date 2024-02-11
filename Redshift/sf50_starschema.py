import redshift_connector
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
    
    # query reversa
queries_ss =  [
    "select d_year, s_city, p_brand, sum(lo_revenue - lo_supplycost) as profit from dwdate, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and (d_year = 1997 or d_year = 1998) and p_category = 'MFGR#14' group by d_year, s_city, p_brand order by d_year, s_city, p_brand;",
    "select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit from dwdate, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region = 'AMERICA' and (d_year = 1997 or d_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, s_nation, p_category order by d_year, s_nation, p_category;",
    "select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit from dwdate, customer, supplier, part, lineorder where lo_custkey = c_custkey  and lo_suppkey = s_suppkey  and lo_partkey = p_partkey  and lo_orderdate = d_datekey  and c_region = 'AMERICA'  and s_region = 'AMERICA'  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, c_nation order by d_year, c_nation;",
    "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where d_yearmonth = 'Dec1997' and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') group by c_city, s_city, d_year order by d_year asc, revenue desc;",
    "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where d_year >= 1992 and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, revenue desc;",
    "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where c_nation = 'UNITED STATES' and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and s_nation = 'UNITED STATES' and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, revenue desc;",
    "select c_nation, s_nation, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year >= 1992 and d_year <= 1997 group by c_nation, s_nation, d_year order by d_year asc, revenue desc;",
    "select sum(lo_revenue), d_year, p_brand from lineorder, dwdate, part, supplier where p_brand = 'MFGR#2221' and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and s_region = 'EUROPE' group by d_year, p_brand order by d_year, p_brand;",
    "select sum(lo_revenue), d_year, p_brand from lineorder, dwdate, part, supplier where p_brand between 'MFGR#2221' and 'MFGR#2228' and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and s_region = 'ASIA' group by d_year, p_brand order by d_year, p_brand;",
    "select sum(lo_revenue), d_year, p_brand from lineorder, dwdate, part, supplier where p_category = 'MFGR#12' and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and s_region = 'AMERICA' group by d_year, p_brand order by d_year, p_brand;",
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_weeknuminyear = 6 and lo_orderdate = d_datekey and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_year = 1993 and lo_orderdate = d_datekey and lo_discount between 1 and 3 and lo_quantity < 25;", 
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_yearmonthnum = 199401 and lo_orderdate = d_datekey and lo_discount between 4 and 6 and lo_quantity between 26 and 35;"
    ]

queries_text_ss = [
    "select d_year, s_city",
    "select d_year, s_nation",
    "select d_year, c_nation",
    "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where d_yearmonth",
    "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where d_year",
    "select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate where c_nation",
    "select c_nation",
    "select sum(lo_revenue), d_year, p_brand from lineorder, dwdate, part, supplier where p_brand =",
    "select sum(lo_revenue), d_year, p_brand from lineorder, dwdate, part, supplier where p_brand between",
    "select sum(lo_revenue), d_year, p_brand from lineorder, dwdate, part, supplier where p_category",
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_weeknuminyear",
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_year",
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_yearmonthnum"
]

datatype = "starschema"
database = "sf50"
size = "sf50"
n_query = 50
retry_count = 0
conn = redshift_connector.connect(host='HOST.us-east-1.redshift.amazonaws.com',database='dev',port=5439,user='fred',password='XXXXXXXXXXX')
if conn:
    try:
        print('Connected')
        cursor = conn.cursor()
        for i in range(0,13):
            exec = []
            start = datetime.today()
            print("start_{}:{}".format(i, start.strftime("%Y-%m-%d %H:%M:%S")))
            for _ in range(0, n_query+1):
                go = True
                while go and retry_count < 5:
                    cursor = conn.cursor()
                    cursor.execute("SET enable_result_cache_for_session = FALSE;")
                    cursor.execute(queries_ss[i])
                    cursor.execute("select starttime, endtime, query from stl_query where querytxt like '"+queries_text_ss[i]+"%' order by query desc limit 1")
                    res_fetchall = cursor.fetchall()
                    start = res_fetchall[0][0]
                    end = res_fetchall[0][1]
                    query = res_fetchall[0][2]
                    delta = end-start
                    #print(query, start, end, delta)
                    cursor.execute("select total_exec_time, est_peak_mem from stl_wlm_query where query = '"+str(query)+"'")
                    res = cursor.fetchall()
                    try:
                        if(len(res) > 0):
                            total_exec_time = res[0][0]/1000
                            mem = res[0][1]
                            res = {"total_exec_time": total_exec_time, "est_peak_mem": mem}
                            pd_res = pd.DataFrame.from_records([res], coerce_float=True)
                            exec.append(pd_res)
                    except:
                        pass
                    else:
                        go = False
                    finally:
                        cursor.close()
            results = pd.concat(exec)
            results.to_csv("data/{}/{}/{}.csv".format(size,datatype,i), index=False, header=True)
            results = results.iloc[1: , :] # eliminando a primeira linha (valor que eh sempre muito alto) para calcular as metricas
            analysis = {"mean": [], "mean_min": [], "mean_max": [], "std": [], "var": [], "cv": [], "min": [], "max": [], "median": []}
            atts = ["total_exec_time", "est_peak_mem"]
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
            pd.DataFrame.from_dict([times]).to_csv("data/{}/{}/times.csv".format(size,datatype), mode="a", index=False, header=not os.path.exists(f"C:/Users/fredrabelo/Desktop/Doutorado/Experimento2/Redshift/data/{size}/{datatype}/times.csv"))
            resultado = {"#": i, "MEAN total_exec_time": ci(results["total_exec_time"])[0], "STD total_exec_time": np.std(results["total_exec_time"][0]), "MEAN est_peak_mem": ci(results["est_peak_mem"])[0], "STD est_peak_mem": np.std(results["est_peak_mem"][0])}
            pd.DataFrame.from_dict([resultado]).to_csv("data/{}/{}/resultado.csv".format(size,datatype), mode="a", index=False, header=not os.path.exists(f"C:/Users/fredrabelo/Desktop/Doutorado/Experimentos/Singlestore/data/{size}/{datatype}/resultado.csv"))

    except Exception as e:
        print(repr(e))
        print ("Retry after 5 sec")
        retry_count = retry_count + 1
        cursor.close()
        conn.close()
        time.sleep(5)
    finally:
        conn.close()
else:
    print("Error: not connected")