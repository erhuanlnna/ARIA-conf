from test_queries import * 

# mark_list = ['SPV1', 'SPV2']
pricer_name_list = ['VPricer', 'PVPricer', 'QAPricer', 'MyPricer']

folder = "rs/"
rs = {}
baseline_name_list  = ["VBP", "PBP","QIRANA", "ARIA"]
for i, name in enumerate(pricer_name_list):
    filename = f"{folder}{db}-{name}-ID1.csv"
    df1 = pd.read_csv(filename)
    
    filename = f"{folder}{db}-{name}-ID2.csv"
    df2 = pd.read_csv(filename)
    
    distinct_rs = df1.iloc[:-1, 2]
    orgin_rs = df2.iloc[:-1, 2]
    #print(orgin_rs)
    ratio_rs = [distinct_rs[i]/orgin_rs[i] for i in range(len(distinct_rs))]
    rs[baseline_name_list[i]] = ratio_rs
    

file_name = f"rs/{db}-dependency.csv"
df = pd.DataFrame(rs, index = ['F1', 'F2', 'F3'])


df.to_csv(file_name, float_format='%.3f')    
print(df)
