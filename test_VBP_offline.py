from VPricer import *
import time
from test_queries import *
import datetime
import sys

now = datetime.datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current time is:", current_time)

repeat_num = 5
pricer= VPricer(db, table_size_list, tuple_price_list)

for mark in mark_sql_list.keys():
    # if("ID" not in mark):
    #    continue
    # if("SPV"  not in mark):
        # continue
    if("A" in mark):
         continue
    # if(mark != "SP"):
        # continue
    # if("A" in mark):
        # continue
    # if(mark != "SPJ"):
        # continue
    rs = defaultdict(list)
    print(f"---------------------------{mark}-------------------------------------")
    file_name = f"rs/{db}-VPricer-{mark}.csv"
    sql_list = mark_sql_list[mark]
    time_list = []
    price_list = []
    for i, sql in enumerate(sql_list):
        o_results = load_pre_query_results(sql, mark, i, db)
        print(len(o_results))
        query_tables = parse_sql_statements(sql)
        is_distinct = "distinct" in sql
        start_time = time.time()
        for _ in range(repeat_num):
            price = pricer.pre_price_SQL_query(is_distinct, o_results, query_tables)
        end_time = time.time()
        # print(sql, price)
        time_list.append((start_time - end_time) / (-repeat_num))
        price_list.append(price)

    rs[f"Time"] = time_list
    rs[f"Price"] = price_list
    rs[f"Time"].append(sum(time_list) /len(time_list))
    rs[f"Price"].append(sum(price_list)/len(price_list))

    df = pd.DataFrame(rs, index = [i for i in range(len(sql_list) + 1)])
    df.to_csv(file_name)
    print(df)
