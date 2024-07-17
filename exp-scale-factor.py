from test_queries import *




name_list = ["VPricer", "PVPricer", "QAPricer",f"MyPricer"]
baseline_name_list  = ["VBP", "PBP", "QIRANA", "ARIA"]
index_list = mark_sql_list.keys()

rs = defaultdict(list)
folder = "pre_rs/"
if 'tpch' in db:
    db_list = ["tpch1g", 'tpch2g', 'tpch3g', 'tpch4g', 'tpch5g']
else:
    db_list = ["ssb1g", 'ssb2g', 'ssb3g', 'ssb4g', 'ssb5g']
scale_factor_list = [1, 2, 3, 4, 5]
rs = defaultdict(list)
mark_list = ['SJ', 'SPJ', 'SAJ']
sj_rs = defaultdict(list)
spj_rs = defaultdict(list)
saj_rs = defaultdict(list)
rs_list = [sj_rs, spj_rs, saj_rs]
for db in db_list:
    folder = "rs/"
    df = pd.read_csv(f"{folder}{db}-compare.csv")
    time_rs = np.array(np.array(df.values)[0:6, -3:], dtype = float)
    print(db, time_rs)
    for i, name in enumerate(name_list):
        for j, mark in enumerate(mark_list):
            rs_list[j][name].append(time_rs[i, j])
        
for j, mark in enumerate(mark_list):
    df = pd.DataFrame(rs_list[j], index = scale_factor_list) 
    file_name = f"rs/tpch-{mark}-scale-factor.csv"
    df.to_csv(file_name, float_format='%.3f')    
    print(df)

for i, name in enumerate(name_list):
    tmp_rs = sj_rs[name] + spj_rs[name] + saj_rs[name]
    tmp_rs = [i/3 for i in tmp_rs]
    rs[name] = tmp_rs

df = pd.DataFrame(rs_list[j], index = scale_factor_list) 
file_name = f"rs/tpch-SPJA-scale-factor.csv"
df.to_csv(file_name, float_format='%.3f')    
print(df)    



        
        
        
