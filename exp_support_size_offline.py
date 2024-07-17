from test_queries import *

var_list = [0, 0.2, 0.4, 0.6, 0.8, 1]
str_var_list = ["", "_ar_support_2", "_ar_support_4", "_ar_support_6", "_ar_support_8", "_ar_support_10"]
name_list = ["", "VPricer", "PVPricer", "QAPricer",f"MyPricer"]
baseline_name_list  = ["Query", "VBP", "PBP", "QIRANA", "ARIA"]
index_list = mark_sql_list.keys()
compare_mark_list = ['S',  'SJ', 'SA', 'SJA', 'SP', 'SPJ']
folder = "pre_rs/"

query_time_dict = defaultdict(list)
my_query_time_dict = defaultdict(list)
process_time_dict = defaultdict(list)
for var_str in str_var_list:
    for mark in mark_sql_list.keys():
        sql_list = mark_sql_list[mark]
        if mark not in compare_mark_list:
            continue
        # only focus on S+SJ+SA+SAJ++SP+SPJ queries
        for i, sql in enumerate(sql_list):
            filename = f"{folder}{db}-{mark}-{i}-time.csv"
            df = pd.read_csv(filename, header=None, na_values=['\\N'])
            query_time = df.values[0][0]
            query_time_dict[mark].append(query_time)
            if var_str != "":
                filename = f"{folder}{db}-{mark}-{i}-MyPricer{var_str}.csv"
                df = pd.read_csv(filename, header=None, na_values=['\\N'])
                add_query_time = df.values[0][0]
            else:
                add_query_time = 0
            my_query_time_dict[f"{mark}{var_str}"].append(query_time + add_query_time)
            
            
          
        avg_time = sum(my_query_time_dict[f"{mark}{var_str}"])/len(sql_list)
        my_query_time_dict[f"{mark}{var_str}"].append(avg_time)    
    

folder = "rs/"

rs = defaultdict(list)
avg_rs = defaultdict(list)
for i, var_str in enumerate(str_var_list):
    avg_time = 0
    avg_price = 0
    cnt = 0
    print("----------------")
    for mark in mark_sql_list.keys():
        if mark not in compare_mark_list:
            continue
        query_time = my_query_time_dict[f"{mark}{var_str}"][-1]
        process_time = 0
        filename = f"{folder}{db}-MyPricer-{mark}{var_str}.csv"
        df = pd.read_csv(filename, header=None, na_values=['\\N'])
        process_time = float(df.values[-1][-2])
        price = float(df.values[-1][-1])
        print(mark, var_str, process_time, query_time, price)
        rs[f"{mark}{str_var_list}"].append(process_time + query_time)
        avg_time += (process_time + query_time) * len(mark_sql_list[mark])
        avg_price  += price * len(mark_sql_list[mark])
        cnt += len(mark_sql_list[mark])
        
    avg_rs[f"Time"].append(avg_time/cnt)
    avg_rs[f"Price"].append(avg_price/cnt)

file_name = f"rs/{db}-MyPricer-support-size-detail-time.csv"
df = pd.DataFrame(rs, index = var_list)
df.to_csv(file_name, float_format='%.3f')    

file_name = f"rs/{db}-MyPricer-size.csv"
df = pd.DataFrame(avg_rs, index = var_list)
df.to_csv(file_name, float_format='%.3f')  
print(df)

        
        
        
