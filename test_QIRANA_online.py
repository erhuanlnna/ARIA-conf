from QAPricer import *
from test_queries import mark_sql_list
import time


repeat_num = 5
support_suffix = "_qa_support"
qa_pricer = QAPricer(db, table_list, table_fields, history, table_price_list, table_size_list, support_suffix, history_aware)

for mark in mark_sql_list.keys():
    #if("ID"  not in mark):
    #    continue
    # if("SAV" not in mark):
        # continue
    # if(mark != "SS"):
        # continue
    # if("A" in mark):
        # continue
    # if(mark != "ID"):
        # continue
    # if("SPV"  not in mark):
        # continue
    
    print(f"---------------------------{mark}-------------------------------------")
    file_name = f"rs/{db}-QAPricer-{mark}.csv"
    sql_list = mark_sql_list[mark]
    rs = defaultdict(list)
    time_list = []
    price_list = []
    for i, sql in enumerate(sql_list):
        print(i, sql)
        start_time = time.time()
        for _ in range(repeat_num):
            price = qa_pricer.price_SQL_query(sql)
        end_time = time.time()
        time_list.append((start_time - end_time) / (-repeat_num))
        price_list.append(price)
        rs[f"Time"].append(time_list[-1])
        rs[f"Price"].append(price_list[-1])
        
    rs[f"Time"].append(sum(time_list) /len(time_list))
    rs[f"Price"].append(sum(price_list)/len(price_list))
    
    df = pd.DataFrame(rs, index = [i for i in range(len(sql_list) + 1)])
    df.to_csv(file_name)
    print(df)
    

    