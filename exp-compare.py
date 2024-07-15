from test_queries import *

name_list = ["Query", "VPricer", "PVPricer", "QAPricer",f"MyPricer"]
baseline_name_list  = ["Query", "VBP", "PBP", "QIRANA", "ARIA"]
index_list = mark_sql_list.keys()


folder = "pre_rs/"

query_time_dict = defaultdict(list)
QA_query_time_dict = defaultdict(list)
process_time_dict = defaultdict(list)
for mark in mark_sql_list.keys():
    sql_list = mark_sql_list[mark]
    if(mark.startswith("SS") or mark.startswith("J") or "V" in mark or "ID" in mark):
        continue
    for i, sql in enumerate(sql_list):
        filename = f"{folder}{db}-{mark}-{i}-time.csv"
        df = pd.read_csv(filename, header=None, na_values=['\\N'])
        query_time = df.values[0][0]
        query_time_dict[mark].append(query_time)
        
        filename = f"{folder}{db}-{mark}-{i}-QAPricer.csv"
        df = pd.read_csv(filename, header=None, na_values=['\\N'])
        add_query_time = df.values[0][0]
        QA_query_time_dict[mark].append(query_time + add_query_time)
    
    avg_time = sum(query_time_dict[mark])/len(sql_list)
    query_time_dict[mark].append(avg_time)    
    avg_time = sum(QA_query_time_dict[mark])/len(sql_list)
    QA_query_time_dict[mark].append(avg_time)    
    
    rs = {} 
    rs["Time"] = query_time_dict[mark]
    df = pd.DataFrame(rs, index = [i for i in range(len(sql_list) + 1)])
    file_name = f"rs/{db}-{mark}-query-time.csv"
    df.to_csv(file_name)
    
    rs = {} 
    rs["Time"] = QA_query_time_dict[mark]
    df = pd.DataFrame(rs, index = [i for i in range(len(sql_list) + 1)])
    file_name = f"rs/{db}-{mark}-QAPricer-query-time.csv"
    df.to_csv(file_name)

folder = "rs/"

rs = defaultdict(list)
for mark in mark_sql_list.keys():
    if(mark.startswith("SS") or mark.startswith("J") or "V" in mark or "ID" in mark):
        continue
    for i, name in enumerate(name_list):
        process_time = 0
        filename = f"{folder}{db}-{name}-{mark}.csv"
        # If the file exists, read the file
        if(name != "QAPricer"):
            query_time = query_time_dict[mark][-1]
        else:
            query_time = QA_query_time_dict[mark][-1]
        try:
            df = pd.read_csv(filename, header=None, na_values=['\\N'])
            process_time = float(df.values[-1][-2])
            
            print(name, mark, process_time, query_time)
            rs[mark].append(process_time + query_time)
            # all_results = df.values
        except FileNotFoundError:
            print(f"No file {filename}")
            if(name == "Query"): 
                rs[mark].append(query_time)
            else:
                rs[mark].append(-1)

if db == 'tpch1g' or db == 'ssb1g':
    rs["S/SJ"] = [(rs["S"][i] * len(mark_sql_list['S']) + rs["SJ"][i] * len(mark_sql_list['SJ']))/(len(mark_sql_list['S']) + len(mark_sql_list['SJ'])) for i in range(len(name_list))]    
    rs["SP/SPJ"] = [(rs["SP"][i] * len(mark_sql_list['SP']) + rs["SPJ"][i] * len(mark_sql_list['SPJ']))/(len(mark_sql_list['SP']) + len(mark_sql_list['SPJ'])) for i in range(len(name_list))]    
    rs["SA/SAJ"] = [(rs["SA"][i] * len(mark_sql_list['SA']) + rs["SJA"][i] * len(mark_sql_list['SJA']))/(len(mark_sql_list['SA']) + len(mark_sql_list['SJA'])) for i in range(len(name_list))]    

file_name = f"rs/{db}-compare.csv"
df = pd.DataFrame(rs, index = baseline_name_list)
df.to_csv(file_name, float_format='%.3f')    
print(df)


        
        
        
