from test_queries import *


name_list = ["VPricer", "PVPricer", "QAPricer",f"MyPricer"]
baseline_name_list  = ["VBP", "PBP", "QIRANA", "ARIA"]
index_list = mark_sql_list.keys()
var_list = [1, 2, 4, 6, 8]

prefix = "JS"
folder = "rs/"


rs = defaultdict(list)
for mark in mark_sql_list.keys():
    if(not mark.startswith(prefix)):
        continue
    for i, name in enumerate(name_list):
        process_time = 0
        filename = f"{folder}{db}-{name}-{mark}.csv"
        try:
            df = pd.read_csv(filename, header=None, na_values=['\\N'])
            process_time = np.array(np.array(df.values)[1:6, 1], dtype = float)
            rs[name] = process_time
        except FileNotFoundError:
            print(f"No file {filename}")
            rs[name] = [-1 for _ in query_time_list]
             
    file_name = f"rs/{db}-{mark}-join.csv"
    df = pd.DataFrame(rs, index = var_list)
    df.to_csv(file_name, float_format='%.3f')    
    print(df)

mark_list = ['JS', 'JSP', 'JSA']        

rs = {}
for mark in mark_list:
    filename = f"{folder}{db}-{mark}-join.csv"
    df = pd.read_csv(filename, header=None, na_values=['\\N'])
    time_rs = np.array(np.array(df.values)[1:6, 1:], dtype = float)
    
    for i, name in enumerate(name_list):
        if(name not in rs.keys()):
            rs[name] = time_rs[:, i]
        else:
            new_time_rs = rs[name] + time_rs[:, i]
            rs[name] = new_time_rs

for i, name in enumerate(name_list):      
    rs[name] = rs[name] / len(mark_list)  
    
# print(rs)
file_name = f"rs/{db}-SPJA-join.csv"
df = pd.DataFrame(rs, index = var_list)
df.to_csv(file_name, float_format='%.3f')    
print(df) 
        
        
        
