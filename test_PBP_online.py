from PVPricer import *
import time
from test_queries import mark_sql_list
import sys

repeat_num = 5
pricer= PVPricer(db, table_size_list, tuple_price_list, table_fields)


for mark in mark_sql_list.keys():
    #if("ID"  not in mark):
    #    continue
    if("A" in mark or "P" in mark):
        continue
    # if(mark != "ID"):
        # continue
    # if("A" in mark):
        # continue
    # if(mark != "SP"):
        # continue
    rs = defaultdict(list)
    print(f"---------------------------{mark}-------------------------------------")
    
    sql_list = mark_sql_list[mark]
    time_list = []
    price_list = []
    for i, sql in enumerate(sql_list):
        start_time = time.time()
        for _ in range(repeat_num):
            price = pricer.price_SQL_query(sql)
        end_time = time.time()
        time_list.append((start_time - end_time) / (-repeat_num))
        price_list.append(price)


    rs[f"Time"] = time_list
    rs[f"Price"] = price_list
    rs[f"Time"].append(sum(time_list) /len(time_list))
    rs[f"Price"].append(sum(price_list)/len(price_list))

    df = pd.DataFrame(rs, index = [i for i in range(len(sql_list) + 1)])
    file_name = f"rs/{db}-PVPricer-{mark}.csv"
    df.to_csv(file_name)
    print(df)

