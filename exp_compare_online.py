from test_queries import *

name_list = ["VPricer", "PVPricer", "QAPricer",f"MyPricer"]
baseline_name_list  = ["VBP", "PBP", "QIRANA", "ARIA"]
index_list = mark_sql_list.keys()
compare_mark_list = ['S',  'SJ', 'SA', 'SJA', 'SP', 'SPJ']

folder = "rs/"

rs = defaultdict(list)
for mark in mark_sql_list.keys():
    if mark not in compare_mark_list:
        continue
    for i, name in enumerate(name_list):
        process_time = 0
        filename = f"{folder}{db}-{name}-{mark}.csv"
        try:
            df = pd.read_csv(filename, header=None, na_values=['\\N'])
            process_time = float(df.values[-1][-2])
            
            #print(name, mark, process_time)
            rs[mark].append(process_time)
            # all_results = df.values
        except FileNotFoundError:
            print(f"No file {filename}")
            rs[mark].append(-1)

if db == 'tpch1g' or db == 'ssb1g':
    rs["S/SJ"] = [(rs["S"][i] * len(mark_sql_list['S']) + rs["SJ"][i] * len(mark_sql_list['SJ']))/(len(mark_sql_list['S']) + len(mark_sql_list['SJ'])) for i in range(len(name_list))]    
    rs["SP/SPJ"] = [(rs["SP"][i] * len(mark_sql_list['SP']) + rs["SPJ"][i] * len(mark_sql_list['SPJ']))/(len(mark_sql_list['SP']) + len(mark_sql_list['SPJ'])) for i in range(len(name_list))]    
    rs["SA/SAJ"] = [(rs["SA"][i] * len(mark_sql_list['SA']) + rs["SJA"][i] * len(mark_sql_list['SJA']))/(len(mark_sql_list['SA']) + len(mark_sql_list['SJA'])) for i in range(len(name_list))]    

file_name = f"rs/{db}-compare.csv"
df = pd.DataFrame(rs, index = baseline_name_list)
df.to_csv(file_name, float_format='%.3f')    
print(df)


        
        
        
