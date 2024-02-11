import mysql.connector
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

base_query = "SELECT CPU_TIME_MS, CPU_WAIT_TIME_MS, ELAPSED_TIME_MS, DISK_LOGICAL_READ_B, DISK_LOGICAL_WRITE_B, DISK_PHYSICAL_READ_B, DISK_PHYSICAL_WRITE_B, 1000*MEMORY_BS/ELAPSED_TIME_MS AVG_MEMORY_USED_B FROM INFORMATION_SCHEMA.mv_query_activities_extended_cumulative WHERE QUERY_TEXT LIKE "

queries_3nf =  [
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_yearmonthnum = 199401 and lo_orderdate = d_datekey and lo_discount between 4 and 6 and lo_quantity between 26 and 35;", 
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_year = 1993 and lo_orderdate = d_datekey and lo_discount between 1 and 3 and lo_quantity < 25;", 
    "select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_weeknuminyear = 6 and lo_orderdate = d_datekey and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    "select sum(lo_revenue), d_year, brand from lineorder, dwdate, part, supplier, brand, category, city, nation, region where category = 'MFGR#12' and category.id = part.p_category_id and brand.id = part.p_brand_id and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and s_city_id = city.city_id and city.nation_id = nation.nation_id and nation.region_id = region.region_id and region_name = 'AMERICA' group by d_year, brand order by d_year, brand;",
    "select sum(lo_revenue), d_year, brand from lineorder, dwdate, part, supplier, brand, city, nation, region where brand between 'MFGR#2221' and 'MFGR#2228' and brand.id = part.p_brand_id and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and s_city_id = city.city_id and city.nation_id = nation.nation_id and nation.region_id = region.region_id and region_name = 'ASIA' group by d_year, brand order by d_year, brand;",
    "select sum(lo_revenue), d_year, brand from lineorder, dwdate, part, supplier, brand, city, nation, region where brand = 'MFGR#2221' and brand.id = part.p_brand_id  and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey  and s_city_id = city.city_id and city.nation_id = nation.nation_id and nation.region_id = region.region_id and region_name = 'EUROPE' group by d_year, brand order by d_year, brand;",
    "select nc.nation_name as c_nation, ns.nation_name as s_nation, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate, city cc, nation nc, region rc, city cs, nation ns, region rs where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_city_id = cc.city_id and cc.nation_id = nc.nation_id and nc.region_id = rc.region_id and rc.region_name = 'ASIA' and s_city_id = cs.city_id and cs.nation_id = ns.nation_id and ns.region_id = rs.region_id and rs.region_name = 'ASIA' and d_year >= 1992 and d_year <= 1997 group by nc.nation_name, ns.nation_name, d_year order by d_year asc, revenue desc;",
    "select cc.city_name as c_city, cs.city_name as s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate, city cc, nation nc, city cs, nation ns where c_city_id = cc.city_id and cc.nation_id = nc.nation_id and nc.nation_name = 'UNITED STATES' and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and s_city_id = cs.city_id and cs.nation_id = ns.nation_id and ns.nation_name = 'UNITED STATES' and d_year >= 1992 and d_year <= 1997 group by cc.city_name, cs.city_name, d_year order by d_year asc, revenue desc;",
    "select cc.city_name as c_city, cs.city_name as s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate, city cc, city cs where c_city_id = cc.city_id and s_city_id = cs.city_id and d_year >= 1992 and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (cc.city_name='UNITED KI1' or cc.city_name='UNITED KI5') and (cs.city_name='UNITED KI1' or cs.city_name='UNITED KI5') and d_year <= 1997 group by cc.city_name, cs.city_name, d_year order by d_year asc, revenue desc;",
    "select cc.city_name as c_city, cs.city_name as s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate, city cc, city cs where c_city_id = cc.city_id and s_city_id = cs.city_id and d_yearmonth = 'Dec1997' and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (cc.city_name='UNITED KI1' or cc.city_name='UNITED KI5') and (cs.city_name='UNITED KI1' or cs.city_name='UNITED KI5') group by cc.city_name, cs.city_name, d_year order by d_year asc, revenue desc;",
    "select d_year, nc.nation_name as c_nation, sum(lo_revenue - lo_supplycost) as profit from dwdate, customer, supplier, part, lineorder, city cc, nation nc, region rc, city cs, nation ns, region rs, manufacturer where lo_custkey = c_custkey  and lo_suppkey = s_suppkey  and lo_partkey = p_partkey  and lo_orderdate = d_datekey and c_city_id = cc.city_id and cc.nation_id = nc.nation_id and nc.region_id = rc.region_id and rc.region_name = 'AMERICA' and s_city_id = cs.city_id and cs.nation_id = ns.nation_id and ns.region_id = rs.region_id and rs.region_name = 'AMERICA'  and part.p_mfgr_id = manufacturer.id and (manufacturer.mfgr = 'MFGR#1' or manufacturer.mfgr = 'MFGR#2') group by d_year, nc.nation_name order by d_year, nc.nation_name;",
    "select d_year, ns.nation_name as s_nation, category, sum(lo_revenue - lo_supplycost) as profit from dwdate, customer, supplier, part, lineorder, city cc, nation nc, region rc, city cs, nation ns, region rs, category, manufacturer where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_city_id = cc.city_id and cc.nation_id = nc.nation_id and nc.region_id = rc.region_id and rc.region_name = 'AMERICA' and s_city_id = cs.city_id and cs.nation_id = ns.nation_id and ns.region_id = rs.region_id and rs.region_name = 'AMERICA' and (d_year = 1997 or d_year = 1998) and part.p_mfgr_id = manufacturer.id and part.p_category_id = category.id and (manufacturer.mfgr = 'MFGR#1' or manufacturer.mfgr = 'MFGR#2') group by d_year, ns.nation_name, category order by d_year, ns.nation_name, category;",
    "select d_year, cs.city_name as s_city, brand, sum(lo_revenue - lo_supplycost) as profit from dwdate, customer, supplier, part, lineorder, city cc, nation nc, region rc, city cs, nation ns, brand, category where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_city_id = cc.city_id and cc.nation_id = nc.nation_id and nc.region_id = rc.region_id and rc.region_name = 'AMERICA' and s_city_id = cs.city_id and cs.nation_id = ns.nation_id and ns.nation_name = 'UNITED STATES' and (d_year = 1997 or d_year = 1998) and part.p_brand_id = brand.id and part.p_category_id = category.id and category = 'MFGR#14' group by d_year, cs.city_name, brand order by d_year, cs.city_name, brand;"
    ]

queries_text_3nf = [
    "\"select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_yearmonthnum%\"",
    "\"select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_year%\"",
    "\"select sum(lo_extendedprice*lo_discount) as revenue from lineorder, dwdate where d_weeknuminyear%\"",
    "\"select sum(lo_revenue), d_year, brand from lineorder, dwdate, part, supplier, brand, category, city, nation, region where category%\"",
    "\"select sum(lo_revenue), d_year, brand from lineorder, dwdate, part, supplier, brand, city, nation, region where brand between%\"",
    "\"select sum(lo_revenue), d_year, brand from lineorder, dwdate, part, supplier, brand, city, nation, region where brand =%\"",
    "\"select nc.nation_name as c_nation%\"",
    "\"select cc.city_name as c_city, cs.city_name as s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate, city cc, nation nc%\"",
    "\"select cc.city_name as c_city, cs.city_name as s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate, city cc, city cs where c_city_id = cc.city_id and s_city_id = cs.city_id and d_year >=%\"",
    "\"select cc.city_name as c_city, cs.city_name as s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, dwdate, city cc, city cs where c_city_id = cc.city_id and s_city_id = cs.city_id and d_yearmonth%\"",
    "\"select d_year, nc.nation_name as c_nation%\"",
    "\"select d_year, ns.nation_name as s_nation%\"",
    "\"select d_year, cs.city_name as s_city%\""
]


host = "svc-HOST.aws-virginia-6.svc.singlestore.com"  #sf50 
user = "admin"
password = "XXXXXXXXXXX" #sf50 
datatype = "3nf"
database = "sf50_starschema"
size = "sf50"
n_query = 40
retry_count = 0
conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
if conn.is_connected():
    try:
        print('Connected')
        cursor = conn.cursor(buffered=True)
        #cursor.execute('SET GLOBAL connect_timeout=6000000')
        #cursor.execute('SET GLOBAL max_allowed_packet=1073741824')
        for i in range(0,13):
            exec = []
            start = datetime.today()
            print("start_{}:{}".format(i, start.strftime("%Y-%m-%d %H:%M:%S")))
            for _ in range(0, n_query+1):
                go = True
                while go and retry_count < 5:
                    cursor = conn.cursor(buffered=True)
                    cursor.execute('DROP ALL FROM PLANCACHE')
                    cursor.execute(queries_3nf[i])
                    cursor.execute(base_query + queries_text_3nf[i])
                    res_fetchall = cursor.fetchall()
                    #print('2')
                    try:
                        res = {"CPU_TIME_MS": int(res_fetchall[0][0]), "ELAPSED_TIME_MS": int(res_fetchall[0][2]), "AVG_MEMORY_USED_B": res_fetchall[0][7]}
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
            results = results.iloc[1: , :] # eliminando a primeira linha (valor que eh sempre muito alto) para calcular as metricas
            analysis = {"mean": [], "mean_min": [], "mean_max": [], "std": [], "var": [], "cv": [], "min": [], "max": [], "median": []}
            atts = ["CPU_TIME_MS", "ELAPSED_TIME_MS", "AVG_MEMORY_USED_B"]
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
            pd.DataFrame.from_dict([times]).to_csv("data/{}/{}/times.csv".format(size,datatype), mode="a", index=False, header=not os.path.exists(f"C:/Users/fredrabelo/Desktop/Doutorado/Experimentos/Singlestore/data/{size}/{datatype}/times.csv"))
            resultado = {"#": i, "MEAN CPU_TIME_MS": ci(results["CPU_TIME_MS"])[0], "STD CPU_TIME_MS": np.std(results["CPU_TIME_MS"][0]), "MEAN ELAPSED_TIME_MS": ci(results["ELAPSED_TIME_MS"])[0], "STD ELAPSED_TIME_MS": np.std(results["ELAPSED_TIME_MS"][0]), "MEAN AVG_MEMORY_USED_B": ci(results["AVG_MEMORY_USED_B"])[0], "STD AVG_MEMORY_USED_B": np.std(results["AVG_MEMORY_USED_B"][0])}
            pd.DataFrame.from_dict([resultado]).to_csv("data/{}/{}/resultado.csv".format(size,datatype), mode="a", index=False, header=not os.path.exists(f"C:/Users/fredrabelo/Desktop/Doutorado/Experimentos/Singlestore/data/{size}/{datatype}/resultado.csv"))
    except Exception as e:
        print(repr(e))
        print ("Retry after 5 sec")
        retry_count = retry_count + 1
        cursor.close()
        conn.close()
        time.sleep(5)
        conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
        cursor = conn.cursor(buffered=True)
    finally:
        conn.close()
else:
    print("Error: not connected")