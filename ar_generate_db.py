import time
from dbUtils import *
import random
from sqlalchemy import create_engine
import pandas as pd 
import numpy as np

db = "walmart"     
s_size = 1000  

table_list, table_size_list, table_fields, primary_fields,primary_domains = get_pre_fields_of_all_tables(database=db)
num_list = {}
data_size = 0
for table in table_list:
    data_size += table_size_list[table]
ratio = s_size/data_size
var_list = [2, 4, 6, 8, 10]
engine = create_engine(f'mysql+pymysql://{user}:{password}@localhost/{db}')
sql_index_list = []
for table in table_list:
    num = int(table_size_list[table] * ratio)
    num_list[table] = num
    query = f"select * from {table} limit {num}"
    print(query)
    df = pd.read_sql_query(query, engine)
    print(df.head(11))
    N = df.shape[0]
    M = df.shape[1]
    random_indices = np.random.randint(0, N, size=(N, M))  # 生成N*M的随机索引数组  
    new_df = df.values[random_indices, np.arange(M)]  # 使用随机索引从原始DataFrame中取值  
    new_df = pd.DataFrame(new_df, columns=df.columns)  # 将NumPy数组转换回DataFrame  
    for col in new_df.columns:
        if(col in primary_fields[table]):
            if df[col].dtype == int:  
                # 如果col是整数类型，将所有ID加上最大值
                new_df[col] = df[col] + primary_domains[f"{table}.{col}"][1]
            elif df[col].dtype == str:  
                # 如果col是字符串类型（或对象类型，它通常用于存储混合类型的数据，但字符串是常见的）  
                # 在所有值前加上“new”的前缀  
                new_df[col] = 'new_' + df[col].astype(str)  # 确保即使已经是字符串也加上前缀  
    new_df['aID'] = [i for i in range(table_size_list[table] + 1, num + 1 + table_size_list[table])] 
    primary_str = ",".join(primary_fields[table])
    #print(new_df.head(11))
    for v in var_list:
        K = int(num*v/10)
        df_to_insert = new_df.head(K) 
        support_name = table + f"_ar_support_{v}"
        df_to_insert.to_sql(name=support_name, con=engine, index=False, if_exists='replace')
        if(K != 0 and primary_str != ""):
            sql = f"ALTER TABLE {support_name} ADD PRIMARY KEY ({primary_str});"
            sql_index_list.append(sql)
# print(num_list)

conn = pymysql.connect(host = host, user=user, passwd=password, database=db)
cursor = conn.cursor()
sql = f"use {db}"

#for sql in sql_index_list:
#    print(sql)
for sql in sql_index_list:
    cursor.execute(sql)
cursor.close()



        