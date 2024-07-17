from dbUtils import *
import os
import pandas as pd
from collections import defaultdict
import numpy as np
import time
import re
from collections import Counter
from parse_sql import QueryMetaData
import json
from pandas.errors import EmptyDataError

db = "employment"

table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
tuple_price_list = defaultdict(int)
history = None
domain_list = None
history_aware = False
for table in table_list:
    tuple_price_list[table] = 1
    
table_price_list = table_size_list    
mark_sql_list = {}



mark = 'S'
sql_list = ["select employees, industry_name, seriesID, display_level, year from employment where employees <= 22300.0 AND seriesID like '%00001'", 
"select year, month, seriesID, industry_name, sort_sequence, naics_code, employees, display_level from employment where seriesID like '%00001' AND employees <= 311700.0", 
'select naics_code, display_level, employees from employment where employees <= 482300.0', 
"select display_level, seriesID, naics_code, employees, year, sort_sequence, month, industry_name from employment where naics_code like '%23'", 
"select month from employment where industry_name = 'Nonresidential specialty trade contractors' AND display_level <= 4", 
"select industry_name, sort_sequence, employees, seriesID from employment where seriesID like 'CEU%'", 
'select employees, display_level, year from employment where month > 10', 
 "select naics_code, display_level, seriesID, industry_name, month, employees, year, sort_sequence from employment where industry_name like 'Mis%' AND seriesID like 'CEU%'", 
'select sort_sequence from employment where employees <= 137200.0', 
 'select seriesID, sort_sequence, employees, naics_code, month, year, industry_name, display_level from employment where display_level <= 4 AND sort_sequence > 882', 
 "select year, employees, seriesID, naics_code, sort_sequence, month, display_level, industry_name from employment where display_level <= 4 AND naics_code like '%533'", 
"select month, industry_name, display_level, seriesID, employees, naics_code, year, sort_sequence from employment where industry_name like 'Ser%' AND display_level <= 4", 
"select industry_name, display_level, sort_sequence, naics_code from employment where year <= 2020 AND employees > 746000.0 AND industry_name = 'Textile mills'", 
'select sort_sequence, year, display_level, seriesID from employment where year > 2020 AND display_level <= 3', 
 "select employees, year, sort_sequence from employment where seriesID like '%00001'", 
'select naics_code, seriesID, month, year from employment where display_level > 4 AND month <= 5 AND sort_sequence > 525', 
 'select industry_name, employees, display_level, sort_sequence, month, seriesID from employment where year <= 2020', 
 'select display_level, year from employment where employees > 427300.0', 
 'select month, employees, display_level, industry_name, naics_code, sort_sequence, seriesID from employment where display_level > 4 AND year > 2020 AND sort_sequence > 9', 
 "select year, sort_sequence from employment where industry_name like 'Edu%' AND month <= 1 AND display_level > 4"]
# ["select employees, industry_name, seriesID, display_level, year from employment where employees <= 22300.0 AND seriesID like '%00001'", 
            # "select year, month, seriesID, industry_name, sort_sequence, naics_code, employees, display_level from employment where seriesID like '%00001' AND employees <= 311700.0", 'select naics_code, display_level, employees from employment where employees <= 482300.0', "select display_level, seriesID, naics_code, employees, year, sort_sequence, month, industry_name from employment where naics_code like '%23'", "select month from employment where industry_name = 'Nonresidential specialty trade contractors' AND display_level <= 4", "select industry_name, sort_sequence, employees, seriesID from employment where seriesID like 'CEU%'", 'select employees, display_level, year from employment where month > 10', "select naics_code, display_level, seriesID, industry_name, month, employees, year, sort_sequence from employment where industry_name like 'Mis%' AND seriesID like 'CEU%'", 'select sort_sequence from employment where employees <= 137200.0', 'select seriesID, sort_sequence, employees, naics_code, month, year, industry_name, display_level from employment where display_level <= 4 AND sort_sequence > 882', "select year, employees, seriesID, naics_code, sort_sequence, month, display_level, industry_name from employment where display_level <= 4 AND naics_code like '%533'", "select month, industry_name, display_level, seriesID, employees, naics_code, year, sort_sequence from employment where industry_name like 'Ser%' AND display_level <= 4", "select industry_name, display_level, sort_sequence, naics_code from employment where year <= 2020 AND employees > 746000.0 AND industry_name = 'Textile mills'", 'select sort_sequence, year, display_level, seriesID from employment where year > 2020 AND display_level <= 3', "select employees, year, sort_sequence from employment where seriesID like '%00001'", 'select naics_code, seriesID, month, year from employment where display_level > 4 AND month <= 5 AND sort_sequence > 525', 'select industry_name, employees, display_level, sort_sequence, month, seriesID from employment where year <= 2020', 'select display_level, year from employment where employees > 427300.0', 'select month, employees, display_level, industry_name, naics_code, sort_sequence, seriesID from employment where display_level > 4 AND year > 2020 AND sort_sequence > 9', "select year, sort_sequence from employment where industry_name like 'Edu%' AND month <= 1 AND display_level > 4"]

mark_sql_list[mark] = sql_list

mark = 'SP'
sql_list = ['select distinct year, industry_name, display_level, employees from employment where employees > 1284700.0 AND sort_sequence <= 481', "select distinct industry_name, sort_sequence, seriesID, display_level, employees, naics_code, month from employment where year > 2020 AND seriesID like 'CEU%'", "select distinct naics_code, employees, sort_sequence, seriesID, year, display_level, industry_name, month from employment where naics_code like '523%'", "select distinct sort_sequence, industry_name, year, display_level, naics_code, employees, seriesID from employment where seriesID like 'CEU%' AND display_level <= 4", "select distinct seriesID, month, industry_name, naics_code, sort_sequence, employees, display_level, year from employment where seriesID = 'CEU3231300001' AND sort_sequence <= 257 AND year <= 2020", "select distinct month, sort_sequence, year, display_level from employment where naics_code like '%453' AND seriesID like '%00001' AND display_level <= 4", 'select distinct seriesID, employees, display_level from employment where display_level > 4', 'select distinct month, sort_sequence, seriesID, year, employees, industry_name, display_level, naics_code from employment where month > 11', "select distinct industry_name, employees from employment where employees > 1411300.0 AND seriesID like '%00001' AND industry_name like '%ities'", "select distinct employees, display_level, naics_code, industry_name, sort_sequence, month, year, seriesID from employment where industry_name like 'Man%'", "select distinct industry_name from employment where industry_name like 'Mac%'", "select distinct month, industry_name from employment where industry_name like '%ctors'", 'select distinct display_level, naics_code, month, year, seriesID from employment where display_level <= 3', "select distinct industry_name, employees, month, naics_code, seriesID, sort_sequence, display_level, year from employment where industry_name like 'Tra%' AND seriesID like 'CEU%'", 'select distinct employees, industry_name, year from employment where month <= 7 AND year <= 2021', "select distinct year, seriesID, employees, industry_name, month from employment where industry_name like 'Mon%' AND naics_code like '562%'", 'select distinct employees, naics_code, year from employment where display_level <= 4', 'select distinct year from employment where sort_sequence <= 417', 'select distinct naics_code, year, industry_name, sort_sequence from employment where month > 2', "select distinct industry_name, naics_code, employees, seriesID, sort_sequence, month from employment where seriesID like '%00001' AND sort_sequence > 836 AND naics_code like '524%'"]


mark_sql_list[mark] = sql_list

mark = 'SA'

sql_list = ["select naics_code, min(month) from employment where seriesID like '%00001' AND display_level <= 2 group by naics_code", 'select industry_name, max(employees) from employment where display_level > 3 group by industry_name', 'select max(year) from employment where month > 12', "select seriesID, min(month) from employment where seriesID = 'CEU9093200001' group by seriesID", "select avg(sort_sequence) from employment where seriesID like '%00001' AND sort_sequence > 871", "select industry_name, min(month) from employment where naics_code like '%t 238' group by industry_name", "select max(employees) from employment where seriesID = 'CEU3133100001' AND industry_name like '%goods' AND year <= 2020", "select max(employees) from employment where naics_code like '%-' AND seriesID like '%00001' AND display_level <= 4", 'select min(month) from employment where month <= 10 AND year <= 2020', 'select industry_name, max(employees) from employment where sort_sequence <= 798 AND employees > 15166400.0 group by industry_name', "select naics_code, min(month) from employment where sort_sequence > 132 AND naics_code like '%55' group by naics_code", "select seriesID, sum(display_level) from employment where industry_name like '%vices' AND seriesID like 'CEU%' group by seriesID", "select naics_code, min(month) from employment where industry_name like 'Mot%' AND seriesID = 'CEU9093161101' group by naics_code", "select avg(sort_sequence) from employment where seriesID like '%00001' AND year <= 2020 AND naics_code = '311'", 'select max(year) from employment where display_level > 4', 'select industry_name, min(month) from employment where year > 2020 group by industry_name', "select max(year) from employment where industry_name like '%ation' AND employees <= 7634100.0", 'select naics_code, avg(sort_sequence) from employment where sort_sequence <= 370 group by naics_code', 'select seriesID, max(employees) from employment where employees > 439700.0 AND month <= 1 group by seriesID', "select seriesID, sum(display_level) from employment where industry_name like 'Gen%' AND employees <= 741900.0 AND month <= 8 group by seriesID"]

mark_sql_list[mark] = sql_list
#
mark = 'SS'
sql_list =['select * from employment where employees <= 386200.0', 'select * from employment where employees <= 849700.0', 'select * from employment where employees <= 2088700.0', 'select * from employment where employees <= 6514800.0', 'select * from employment where employees <= 153177000.0']

mark = 'SPV1'
sql_list = []
for table in table_list:
    for field in table_fields[table]:
        if(field != 'aID'):
            sql_list.append(f"select {field} from {table}")
            
mark_sql_list[mark] = sql_list
mark = 'SPV2'
sql_list = []
for table in table_list:
    for field in table_fields[table]:
        if(field != 'aID'):
            sql_list.append(f"select distinct {field} from {table}")
mark_sql_list[mark] = sql_list

mark = 'SAV1'
sql_list = [
    'select count(*) from employment',
    'select max(employees) from employment',
    'select min(employees) from employment',
    'select avg(employees) from employment',
    ]
mark_sql_list[mark] = sql_list

mark = 'SAV2'
sql_list = [
    'select employees from employment',
    ]
mark_sql_list[mark] = sql_list



