from dbUtils import *
import random
import string
import json
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
db = 'walmart'
table = "walmart"
# Create a database engine
engine = create_engine(f'mysql+pymysql://user:user@localhost/walmart')

# Write the SQL query
query = f"SELECT * FROM {table}" 

# Read the data into a DataFrame
df = pd.read_sql_query(query, engine)

num = 1000
support_name = table + "_qa_support"
support_sets = []
support_set_list = []
table_size = df.shape[0]
column_num = df.shape[1]

for i in range(num):
    j = random.randint(0, table_size - 1)
    data = df.iloc[j]
    p1 = random.uniform(0, 1)
    if(p1 <= 0.5): # generate N1 neighborhodd
        while(True):
            k = random.randint(0, column_num - 2) + 1
            series = df.iloc[:, k]
            new_v = np.random.choice(series.dropna().drop_duplicates().tolist())
            if(new_v != data[k]):
                break
        support_set_list.append([df.columns[k], j + 1, j + 1])
        insert_data = list(data) + [i]
        # support_sets.append(insert_data) 
        insert_data[k] = new_v
        support_sets.append(insert_data) 
    else:
        while(True):
            j2 = random.randint(0, table_size - 1)
            n_data = df.iloc[j2]
            k = random.randint(0, column_num - 2) + 1 # ignore the first aID column
            v = data[k]
            new_v = n_data[k]
            if(new_v != v):
                break
        if(j < j2):
            support_set_list.append([df.columns[k], j + 1, j2 + 1])
            insert_data = list(data) + [i]
            insert_data[k] = new_v
            support_sets.append(insert_data) 
            insert_data = list(n_data) + [i]
            insert_data[k] = v
            support_sets.append(insert_data) 
        else:
            support_set_list.append([df.columns[k], j2 + 1, j + 1])
            insert_data = list(n_data) + [i]
            insert_data[k] = v
            support_sets.append(insert_data) 
            insert_data = list(data) + [i]
            insert_data[k] = new_v
            support_sets.append(insert_data) 

# write support set to the file
print("Start writing")
file_name = db +"_" + support_name + ".json"
with open(file_name, 'w') as file:
    json.dump(support_set_list, file)

# print(df.columns, len(df.columns), len(support_sets[0]))
column_name = list(df.columns)
column_name.append('sID')

new_df = pd.DataFrame(support_sets, columns= column_name)
new_df.to_sql(name=support_name, con=engine, index=False, if_exists='replace')
print("Complete")


