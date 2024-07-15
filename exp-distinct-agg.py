from test_queries import * 

# mark_list = ['SPV1', 'SPV2']
pricer_name_list = ['QAPricer', 'MyPricer', 'VPricer']

folder = "rs/"
rs = {}
for name in pricer_name_list:
    filename = f"{folder}{db}-{name}-SPV2.csv"
    df1 = pd.read_csv(filename)
    
    filename = f"{folder}{db}-{name}-SPV1.csv"
    df2 = pd.read_csv(filename)
    
    distinct_rs = df1.iloc[:-1, 2]
    orgin_rs = df2.iloc[:-1, 2]
    print(orgin_rs)
    ratio_rs = [distinct_rs[i]/orgin_rs[i] for i in range(len(distinct_rs))]
    rs[name] = ratio_rs
    
baseline_name_list  = ["QIRANA", "ARIA", "VBP", "Query"]
rs['distinct_rate'] = ratio_rs 
file_name = f"rs/{db}-distinct.csv"
df = pd.DataFrame(rs, table_fields[table_list[0]])

last_column = df.columns[-1]  
new_columns = [last_column] + df.columns[:-1].tolist()  
df = df.reindex(columns=new_columns)  
df.to_csv(file_name, float_format='%.3f')    
print(df)

folder = "rs/"
rs = {}
pricer_name_list = ['QAPricer', 'MyPricer']
for name in pricer_name_list:
    filename = f"{folder}{db}-{name}-SAV1.csv"
    df1 = pd.read_csv(filename)
    
    filename = f"{folder}{db}-{name}-SAV2.csv"
    df2 = pd.read_csv(filename)
    
    distinct_rs = df1.iloc[:-1, 2]
    orgin_rs = df2.iloc[:-1, 2]
    print(orgin_rs)
    ratio_rs = [distinct_rs[i]/orgin_rs[0] for i in range(len(distinct_rs))]
    rs[name] = ratio_rs

for name in pricer_name_list:
    max_ratio = rs[name][1]
    min_ratio = rs[name][2]
    rs[name][1] = min_ratio
    rs[name][2] = max_ratio
    

baseline_name_list  = ["QIRANA", "ARIA"]
file_name = f"rs/{db}-agg.csv"
df = pd.DataFrame(rs, index = ['count', 'min', 'max', 'avg'])


df.to_csv(file_name, float_format='%.6f')    
print(df)