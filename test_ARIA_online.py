import time
from MyPricer import *
from test_queries import mark_sql_list
import argparse
import sys
 
repeat_num = 5

support_size_list = {}
size = 0.2 * 0
r_support = ""
if size != 0:
    r_support = f"_ar_support_{int(size * 10)}"
s_size = 1000
data_size = 0
for table in table_list:
    data_size += table_size_list[table]
ratio = s_size/data_size * size
for table in table_list:
    num = int(table_size_list[table] * ratio)
    support_size_list[table] = num + table_size_list[table]
    
#print(table_size_list, support_size_list)
pricer= Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, r_support, history_aware)

compare_marks = ['S',  'SJ',  'SP',  'SPJ',  'SJA', 'SA']
compare_db_list = ['tpch2g', 'tpch3g', 'tpch4g', 'tpch5g', 'ssb2g', 'ssb3g', 'ssb4g', 'ssb5g']
for mark in mark_sql_list.keys():
    # only run the basic queries when the database is in compare_db_list or size != 0
    if (size or db in compare_db_list) and mark not in compare_marks:
        continue
    #if("ID"  not in mark):
    #    continue
    # if("J" in mark):
        # continue
    # if("A" not in mark):
        # continue
    # if(mark != "ID"):
        # continue
    # if("A" in mark):
        # continue
    # if(mark != "JSA"):
        # continue
    #if(mark.startswith("SS")):
    #    continue
    #if(mark.startswith("JS")):
    #    continue
    print(f"---------------------------{mark}-------------------------------------")
    file_name = f"rs/{db}-MyPricer-{mark}{r_support}.csv"
    sql_list = mark_sql_list[mark]
    time_list = []
    price_list = []
    
    
    rs = defaultdict(list)
    time_list = []
    price_list = []
    for i, sql in enumerate(sql_list):
        
        start_time = time.time()
        for _ in range(repeat_num):
            price = pricer.price_SQL_query(sql)
        end_time = time.time()
        # print(sql,price)
        time_list.append((start_time - end_time) / (-repeat_num))
        price_list.append(price)
        rs[f"Time"].append(time_list[-1])
        rs[f"Price"].append(price_list[-1])
        
    rs[f"Time"].append(sum(time_list) /len(time_list))
    rs[f"Price"].append(sum(price_list)/len(price_list))
    
    df = pd.DataFrame(rs, index = [i for i in range(len(sql_list) + 1)])
    df.to_csv(file_name)
    print(df)
