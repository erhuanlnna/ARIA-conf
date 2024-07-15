from scipy.stats import pearsonr  
import time
from dbUtils import *
import random
from sqlalchemy import create_engine
import pandas as pd 
import numpy as np
from QAPricer import load_support_set
from collections import defaultdict
db = "walmart"
table_list, table_size_list, table_fields, primary_fields,primary_domains = get_pre_fields_of_all_tables(database=db)
data_size = 0
for table in table_list:
    data_size += table_size_list[table]
engine = create_engine(f'mysql+pymysql://{user}:{password}@localhost/{db}')
support_suffix = "_qa_support"
support_sets = load_support_set(table_list, db, support_suffix)

corr_list = {'ARIA': [], 'QIRANA': []}
for table in table_list:
    query = f"select * from {table}"
    print(query)
    df = pd.read_sql_query(query, engine)
    df = df.drop('aID', axis=1)
    N = df.shape[0]

    price_list = []
    entropy_list = []
    for col in df.columns:  
        if(col == 'aID'):
            continue 
        value_counts = df[col].value_counts()
        cnt_list = value_counts.values 
        removed_list = (N - cnt_list)/(N - 1)
        price = np.dot(cnt_list, removed_list)
        price_list.append(price)
        cnt_list = cnt_list/N
        entropy = -np.sum(cnt_list * np.log2(cnt_list))
        entropy_list.append(entropy)
    

    corr, _ = pearsonr(price_list, entropy_list)
    corr_list['ARIA'].append(corr)
    rs = {}
    rs['entropy'] = entropy_list
    rs['ARIA'] = price_list
    price_list = {col: 0 for col in df.columns}
    support_set = support_sets[table]
    support_num = len(support_set)
    for sid in range(support_num):
        support = support_set[sid]
        col = support[0]
        if(col == 'aID'):
            continue 
        price_list[col] += 1
    price_list = list(price_list.values())
    rs['QIRANA'] = price_list    
    corr, _ = pearsonr(price_list, entropy_list)
    corr_list['QIRANA'].append(corr)
    
    rs = pd.DataFrame(rs, index = df.columns)
    rs.to_csv(f"rs/{db}-{table}-entropy-price.csv", float_format='%.6f') 

corr_list['ARIA'].append(np.nanmean(corr_list['ARIA']))
corr_list['QIRANA'].append(np.nanmean(corr_list['QIRANA']))
corr_list = pd.DataFrame(corr_list, index = table_list + ['AVG'])
corr_list.to_csv(f"rs/{db}-corr.csv", float_format='%.6f') 

print(corr_list)
    
    


    

        
    
       
