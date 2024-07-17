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
db = "ssb1g"

table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
tuple_price_list = defaultdict(int)
history = None
domain_list = {
    "dates.d_year": [1990, 2000],
    "lineorder.lo_revenue":[0, 2e+8],
    "lineorder.lo_quantity": [0, 50],
    "lineorder.lo_tax": [0, 8]
    }
history_aware = False
for table in table_list:
    tuple_price_list[table] = 1
    
table_price_list = table_size_list    
mark_sql_list = {}


# SJ
mark = 'SJ'
# sql_list = [
    # "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_year = '1995' and lo_discount between 1 and 10 and lo_quantity < 50;",
    # "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_yearmonthnum = '199401' and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
    # "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    # "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2220' and 'MFGR#2229' and s_region = 'ASIA';",
    # "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand = 'MFGR#2221' and s_region = 'EUROPE';",
    # "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year between 1992 and 1997;",
    # "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and (c_city = 'UNITED KI5' or c_city = 'UNITED KI1') and (s_city = 'UNITED KI5' or s_city = 'UNITED KI1') and d_yearmonth = 'Dec1997';",
    # "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and p_category = 'MFGR#14' and (d_year = 1997 or d_year = 1998);"
# ]

sql_list = [
    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_year = '1995' and lo_discount between 1 and 10 and lo_quantity < 50;",
    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_yearmonthnum = '199401' and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA';",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2220' and 'MFGR#2229' and s_region = 'ASIA';",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand = 'MFGR#2221' and s_region = 'EUROPE';",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_nation, s_nation from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_region = 'ASIA' and s_region = 'ASIA' and d_year <= 1997;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year between 1992 and 1997;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and (c_city = 'UNITED KI5' or c_city = 'UNITED KI1') and (s_city = 'UNITED KI5' or s_city = 'UNITED KI1') and d_yearmonth = 'Dec1997';",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2');",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') and (d_year = 1997 or d_year = 1998);",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and p_category = 'MFGR#14' and (d_year = 1997 or d_year = 1998);"
] 

mark_sql_list[mark] = sql_list



mark = 'SJA'

sql_list = [
    "select lo_orderdate, d_datekey, lo_orderkey, count(*) from lineorder, dates where lo_orderdate = d_datekey and d_year = '1995' and lo_discount between 1 and 10 and lo_quantity < 50 group by lo_orderdate, d_datekey, lo_orderkey;",
    "select lo_orderdate, d_datekey, lo_orderkey, avg(lo_revenue) from lineorder, dates where lo_orderdate = d_datekey and d_yearmonthnum = '199401' and lo_discount between 4 and 6 and lo_quantity between 26 and 35 group by lo_orderdate, d_datekey, lo_orderkey;",
    "select lo_orderdate, d_datekey, lo_orderkey, max(lo_revenue)  from lineorder, dates where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35 group by lo_orderdate, d_datekey, lo_orderkey;",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, min(lo_revenue)  from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA' group by lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey;",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, min(d_year) from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2220' and 'MFGR#2229' and s_region = 'ASIA' group by lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey;",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, max(d_year) from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand = 'MFGR#2221' and s_region = 'EUROPE' group by lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, count(*) from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_region = 'ASIA' and s_region = 'ASIA' and d_year <= 1997 group by lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, avg(lo_quantity) from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year between 1992 and 1997 group by lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, max(lo_quantity) from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and (c_city = 'UNITED KI5' or c_city = 'UNITED KI1') and (s_city = 'UNITED KI5' or s_city = 'UNITED KI1') and d_yearmonth = 'Dec1997' group by lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, min(lo_tax) from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, count(*) from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') and (d_year = 1997 or d_year = 1998) group by lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, min(lo_revenue) from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and p_category = 'MFGR#14' and (d_year = 1997 or d_year = 1998) group by lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey;"
]

mark_sql_list[mark] = sql_list

mark = 'S'
sql_list = [
    "select * from dates where d_year <= 1999",
    "select lo_shipmode, lo_revenue from lineorder where lo_revenue < 10000000",
    "select p_mfgr, p_size, p_color  from part where p_size >= 40 and p_color = 'blue'",
    "select s_region, s_address, s_city from supplier where s_region like 'A%'",
    "select c_mktsegment, c_nation, c_phone from customer where c_nation like 'C%'",
]

mark_sql_list[mark] = sql_list

mark = 'SP'
sql_list = [
    "select distinct * from dates where d_year <= 1999",
    "select distinct lo_shipmode from lineorder where lo_revenue < 10000000",
    "select distinct p_mfgr, p_size, p_color  from part where p_size >= 40 and p_color = 'blue'",
    "select distinct s_region, s_address, s_city from supplier where s_region like 'A%'",
    "select distinct c_mktsegment, c_nation, c_phone from customer where c_nation like 'C%'",
]

mark_sql_list[mark] = sql_list

mark = 'SA'
sql_list = [
    "select count(*) from dates where d_year <= 1999",
    "select lo_shipmode, avg(lo_revenue) from lineorder where lo_revenue < 10000000 group by lo_shipmode",
    # select lo_shipmode, avg(lo_revenue), sum(lo_revenue), count(*) from lineorder where lo_revenue > 10000000 group by lo_shipmode
    "select lo_shipmode, max(lo_revenue) from lineorder group by lo_shipmode",
    "select p_mfgr, count(*) from part where p_size >= 40 and p_color = 'blue' group by p_mfgr",
    "select s_region, count(*) from supplier where s_region like 'A%' group by s_region",
    "select c_mktsegment, c_nation, count(*) from customer where c_nation like 'C%' group by c_mktsegment, c_nation"
]

mark_sql_list[mark] = sql_list

mark = 'SPJ'
sql_list = [
    "select distinct lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_year = '1995' and lo_discount between 1 and 10 and lo_quantity < 50;",
    "select distinct lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_yearmonthnum = '199401' and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
    "select distinct lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    "select distinct lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA';",
    "select distinct lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2220' and 'MFGR#2229' and s_region = 'ASIA';",
    "select distinct lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand = 'MFGR#2221' and s_region = 'EUROPE';",
    "select distinct lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_nation, s_nation from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_region = 'ASIA' and s_region = 'ASIA' and d_year <= 1997;",
    "select distinct lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year between 1992 and 1997;",
    "select distinct lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and (c_city = 'UNITED KI5' or c_city = 'UNITED KI1') and (s_city = 'UNITED KI5' or s_city = 'UNITED KI1') and d_yearmonth = 'Dec1997';",
    "select distinct lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2');",
    "select distinct lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') and (d_year = 1997 or d_year = 1998);",
    "select distinct lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and p_category = 'MFGR#14' and (d_year = 1997 or d_year = 1998);"
] 

mark_sql_list[mark] = sql_list

mark = 'SS'
sql_list = [
    'select lo_shipmode from lineorder where lo_revenue <= 1435095',
    'select lo_shipmode from lineorder where lo_revenue <= 2801124',
    'select lo_shipmode from lineorder where lo_revenue <= 4167278',
    'select lo_shipmode from lineorder where lo_revenue <= 5657210',
    'select lo_shipmode from lineorder where lo_revenue <= 10474950',
]

mark_sql_list[mark] = sql_list

mark = 'SSP'
sql_list = [
    'select distinct lo_shipmode from lineorder where lo_revenue <= 1435095',
    'select distinct lo_shipmode from lineorder where lo_revenue <= 2801124',
    'select distinct lo_shipmode from lineorder where lo_revenue <= 4167278',
    'select distinct lo_shipmode from lineorder where lo_revenue <= 5657210',
    'select distinct lo_shipmode from lineorder where lo_revenue <= 10474950',
]

mark_sql_list[mark] = sql_list

mark = 'SSA'
sql_list = [
    'select avg(lo_revenue) from lineorder where lo_revenue <= 1435095',
    'select avg(lo_revenue) from lineorder where lo_revenue <= 2801124',
    'select avg(lo_revenue) from lineorder where lo_revenue <= 4167278',
    'select avg(lo_revenue) from lineorder where lo_revenue <= 5657210',
    'select avg(lo_revenue) from lineorder where lo_revenue <= 10474950',
]

mark_sql_list[mark] = sql_list

mark = 'JS'
sql_list = [
    'select * from lineorder where lo_revenue <= 2801124',
    'select * from lineorder, dates where lo_orderdate = d_datekey and lo_revenue <= 2801124',
    'select * from lineorder, dates, part where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124',
    'select * from lineorder, dates, part, supplier where lo_suppkey = s_suppkey and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124',
    'select * from lineorder, dates, part, supplier, customer where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124',
]

mark_sql_list[mark] = sql_list

mark = 'JSP'
sql_list = [
    'select distinct lo_shipmode from lineorder where lo_revenue <= 2801124',
    'select distinct lo_shipmode, lo_orderdate, d_datekey from lineorder, dates where lo_orderdate = d_datekey and lo_revenue <= 2801124',
    'select distinct lo_shipmode, lo_orderdate, d_datekey, lo_partkey, p_partkey from lineorder, dates, part where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124',
    'select distinct lo_shipmode, lo_suppkey, s_suppkey, lo_orderdate, d_datekey, lo_partkey, p_partkey from lineorder, dates, part, supplier where lo_suppkey = s_suppkey and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124',
    'select distinct lo_shipmode, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_orderdate, d_datekey, lo_partkey, p_partkey from lineorder, dates, part, supplier, customer where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124',
]

mark_sql_list[mark] = sql_list

mark = 'JSA'
sql_list = [
    'select lo_shipmode, avg(lo_revenue) from lineorder where lo_revenue <= 2801124 group by lo_shipmode',
    'select lo_shipmode, lo_orderdate, d_datekey, avg(lo_revenue) from lineorder, dates where lo_orderdate = d_datekey and lo_revenue <= 2801124 group by lo_shipmode, lo_orderdate, d_datekey',
    'select lo_shipmode, lo_orderdate, d_datekey, lo_partkey, p_partkey, avg(lo_revenue) from lineorder, dates, part where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124 group by lo_shipmode, lo_orderdate, d_datekey, lo_partkey, p_partkey',
    'select lo_shipmode, lo_suppkey, s_suppkey, lo_orderdate, d_datekey, lo_partkey, p_partkey, avg(lo_revenue) from lineorder, dates, part, supplier where lo_suppkey = s_suppkey and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124 group by lo_shipmode, lo_suppkey, s_suppkey, lo_orderdate, d_datekey, lo_partkey, p_partkey',
    'select lo_shipmode, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_orderdate, d_datekey, lo_partkey, p_partkey, avg(lo_revenue) from lineorder, dates, part, supplier, customer where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_revenue <= 2801124 group by lo_shipmode, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_orderdate, d_datekey, lo_partkey, p_partkey',
]

mark_sql_list[mark] = sql_list

