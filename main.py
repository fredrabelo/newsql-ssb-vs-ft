
import connmysql
import sys
from datetime import datetime
from queries import query
from queries import executeQuery
from queries import timedQuery

import pandas as pd

def main(query_type, query_id, database='sf1', bd='singlestore'):

    today_str = datetime.today().strftime("%Y-%m-%d")

    query_id = int(query_id)
    exec_list = []
        
    for i in range(1, 31):
        exec_list.append(timedQuery(query_type, query_id, database, bd, i))

    results = pd.concat(exec_list)
    results["date"] = today_str

    results.to_csv("data/{}_{}_{}_results.csv".format(query_type,query_id,database), mode="a")

if __name__ == '__main__':
    main(*sys.argv[1:])
