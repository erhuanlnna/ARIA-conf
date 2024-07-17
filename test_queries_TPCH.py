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

db = "tpch1g"

table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)

for table in table_fields:
    field_list = []
    for field in table_fields[table]:
        field_list.append(field.upper())
        field_list.append(field.lower())
    table_fields[table] = field_list

tuple_price_list = defaultdict(int)
history = None
domain_list = {
    "lineitem.l_quantity": [0, 50],
    "lineitem.l_discount": [0, 0.15],
    "orders.o_totalprice":[500, 6e+5],  #555285.16 |            857.71 |
    "partsupp.ps_supplycost" : [0, 1000],
    "partsupp.ps_availqty" : [0, 1e+4],
    }
history_aware = False
for table in table_list:
    tuple_price_list[table] = 1
    
table_price_list = table_size_list    
mark_sql_list = {}

mark = 'S'
sql_list = [
    "select l_shipdate from   lineitem where   l_shipdate <= date '1998-09-01'",
    "select p_brand, p_type from part where p_brand = 'Brand#23'   and p_container = 'MED BOX'",
    "select o_orderkey, o_orderstatus, o_totalprice from orders where o_orderdate >= date '1993-07-01'   and o_orderdate < date '1993-07-01' + interval '3' month",
    "select ps_supplycost from partsupp where ps_supplycost > 100",
    "select r_name from region where r_name like 'A%'"
    ]



mark_sql_list[mark] = sql_list

mark = 'SP'
sql_list = [
    "select distinct l_shipdate from lineitem where l_shipdate <= date '1998-09-01'",
    "select distinct p_brand, p_type from part where p_brand = 'Brand#23'   and p_container = 'MED BOX'",
    "select distinct o_orderkey, o_orderstatus, o_totalprice from orders where o_orderdate >= date '1993-07-01'   and o_orderdate < date '1993-07-01' + interval '3' month",
    "select distinct ps_supplycost from partsupp where ps_supplycost > 100",
    "select distinct r_name from region where r_name like 'A%'"
    ]

mark_sql_list[mark] = sql_list

mark = 'SA'
sql_list = [
    "select sum(l_quantity) from lineitem where l_shipdate <= date '1998-09-01'",
    "select p_brand, p_type, count(*) from part where p_brand = 'Brand#23' and p_container = 'MED BOX' group by p_brand, p_type",
    "select o_orderstatus, avg(o_totalprice) from orders where o_orderdate >= date '1993-07-01'   and o_orderdate < date '1993-07-01' + interval '3' month group by o_orderstatus",
    "select max(ps_supplycost) from partsupp where ps_supplycost > 100",
    "select count(*) from region where r_name like 'A%'"
    ]

mark_sql_list[mark] = sql_list
# SJ
mark = 'SJ'
sql_list = [
    "select l_partkey, p_partkey from   lineitem,   part where   p_partkey = l_partkey   and p_brand = 'Brand#23'   and p_container = 'MED BOX'   and l_quantity < 5.10736 ",
    "select l_orderkey, o_orderkey, o_orderpriority, l_commitdate, l_receiptdate from orders, lineitem where   o_orderdate >= date '1993-07-01'   and o_orderdate < date '1993-07-01' + interval '3' month   and l_orderkey = o_orderkey   and l_commitdate < l_receiptdate; ",
    "select o_orderkey,l_orderkey, l_shipmode,o_orderpriority   from   orders,   lineitem where   o_orderkey = l_orderkey   and (l_shipmode = 'MAIL' or l_shipmode = 'SHIP')   and l_commitdate < l_receiptdate   and l_shipdate < l_commitdate   and l_receiptdate >= date '1994-01-01'   and l_receiptdate < date '1994-01-01' + interval '1' year;",
    "select ps_partkey, p_partkey, p_type from   partsupp,   part where   p_partkey = ps_partkey   and p_brand <> 'Brand#45'   and p_type not like 'MEDIUM POLISHED%'   and p_size in (49, 14, 23, 45, 19, 3, 36, 9);",
    "select l_orderkey, o_orderkey,c_custkey,o_custkey, l_suppkey, s_suppkey,c_nationkey, n_nationkey,s_nationkey, n_regionkey, r_regionkey from   customer,   orders,   lineitem,   supplier,   nation,   region where   c_custkey = o_custkey   and l_orderkey = o_orderkey   and l_suppkey = s_suppkey   and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = 'ASIA'   and o_orderdate >= date '1994-01-01'   and o_orderdate < date '1994-01-01' + interval '1' year;",        
    "select p_partkey,ps_partkey,s_suppkey,ps_suppkey,s_nationkey,n_nationkey,r_regionkey,s_acctbal,s_name,n_name   from   part,   supplier,   partsupp,   nation,   region where   p_partkey = ps_partkey   and s_suppkey = ps_suppkey   and p_size = 15   and p_type like '%BRASS'   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = 'EUROPE'   and ps_supplycost = 100;"
]

mark_sql_list[mark] = sql_list

mark = 'SPJ'
sql_list = [
    "select  distinct l_partkey, p_partkey from   lineitem,   part where   p_partkey = l_partkey   and p_brand = 'Brand#23'   and p_container = 'MED BOX'   and l_quantity < 5.10736 ",
    "select distinct l_orderkey, o_orderkey, o_orderpriority, l_commitdate, l_receiptdate from orders, lineitem where   o_orderdate >= date '1993-07-01'   and o_orderdate < date '1993-07-01' + interval '3' month   and l_orderkey = o_orderkey   and l_commitdate < l_receiptdate; ",
    "select distinct o_orderkey,l_orderkey, l_shipmode,o_orderpriority   from   orders,   lineitem where   o_orderkey = l_orderkey   and (l_shipmode = 'MAIL' or l_shipmode = 'SHIP')   and l_commitdate < l_receiptdate   and l_shipdate < l_commitdate   and l_receiptdate >= date '1994-01-01'   and l_receiptdate < date '1994-01-01' + interval '1' year;",
    "select  distinct ps_partkey, p_partkey, p_type from   partsupp,   part where   p_partkey = ps_partkey   and p_brand <> 'Brand#45'   and p_type not like 'MEDIUM POLISHED%'   and p_size in (49, 14, 23, 45, 19, 3, 36, 9);",
    "select distinct l_orderkey, o_orderkey,c_custkey,o_custkey, l_suppkey, s_suppkey,c_nationkey, n_nationkey,s_nationkey, n_regionkey, r_regionkey from   customer,   orders,   lineitem,   supplier,   nation,   region where   c_custkey = o_custkey   and l_orderkey = o_orderkey   and l_suppkey = s_suppkey   and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = 'ASIA'   and o_orderdate >= date '1994-01-01'   and o_orderdate < date '1994-01-01' + interval '1' year;",        
    "select distinct p_partkey,ps_partkey,s_suppkey,ps_suppkey,s_nationkey,n_nationkey,r_regionkey,s_acctbal,s_name,n_name   from   part,   supplier,   partsupp,   nation,   region where   p_partkey = ps_partkey   and s_suppkey = ps_suppkey   and p_size = 15   and p_type like '%BRASS'   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = 'EUROPE'   and ps_supplycost = 100;"
]

mark_sql_list[mark] = sql_list

mark = 'SJA'
sql_list = [
    "select l_partkey, p_partkey, count(*) from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23'   and p_container = 'MED BOX'   and l_quantity < 5.10736 group by l_partkey, p_partkey",
    "select l_orderkey, o_orderkey, count(*) from orders, lineitem where   o_orderdate >= date '1993-07-01'   and o_orderdate < date '1993-07-01' + interval '3' month   and l_orderkey = o_orderkey   and l_commitdate < l_receiptdate group by l_orderkey, o_orderkey; ",
    "select o_orderkey,l_orderkey, l_shipmode, avg(l_discount)   from   orders,   lineitem where   o_orderkey = l_orderkey   and (l_shipmode = 'MAIL' or l_shipmode = 'SHIP')   and l_commitdate < l_receiptdate   and l_shipdate < l_commitdate   and l_receiptdate >= date '1994-01-01'   and l_receiptdate < date '1994-01-01' + interval '1' year group by o_orderkey,l_orderkey, l_shipmode;",
    "select ps_partkey, p_partkey, p_type, max(ps_availqty) from   partsupp,   part where   p_partkey = ps_partkey   and p_brand <> 'Brand#45'   and p_type not like 'MEDIUM POLISHED%'   and p_size in (49, 14, 23, 45, 19, 3, 36, 9) group by ps_partkey, p_partkey, p_type;",
    "select l_orderkey, o_orderkey,c_custkey,o_custkey, l_suppkey, s_suppkey,c_nationkey, n_nationkey,s_nationkey, n_regionkey, r_regionkey, sum(l_quantity) from   customer,   orders,   lineitem,   supplier,   nation,   region where   c_custkey = o_custkey   and l_orderkey = o_orderkey   and l_suppkey = s_suppkey   and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = 'ASIA'   and o_orderdate >= date '1994-01-01'   and o_orderdate < date '1994-01-01' + interval '1' year group by l_orderkey, o_orderkey,c_custkey,o_custkey, l_suppkey, s_suppkey,c_nationkey, n_nationkey,s_nationkey, n_regionkey, r_regionkey;",        
    "select p_partkey,ps_partkey,s_suppkey,ps_suppkey,s_nationkey,n_nationkey,r_regionkey,s_acctbal,s_name,n_name, count(*)   from   part,   supplier,   partsupp,   nation,   region where   p_partkey = ps_partkey   and s_suppkey = ps_suppkey   and p_size = 15   and p_type like '%BRASS'   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = 'EUROPE'   and ps_supplycost = 100 group by p_partkey,ps_partkey,s_suppkey,ps_suppkey,s_nationkey,n_nationkey,r_regionkey,s_acctbal,s_name,n_name,;"
]

mark_sql_list[mark] = sql_list


mark = 'SS'
sql_list = [
    'select l_shipmode from lineitem where l_quantity <= 11.0',
    'select l_shipmode from lineitem where l_quantity <= 21.0',
    'select l_shipmode from lineitem where l_quantity <= 31.0',
    'select l_shipmode from lineitem where l_quantity <= 41.0',
    'select l_shipmode from lineitem where l_quantity <= 50.0',
]

mark_sql_list[mark] = sql_list

mark = 'SSP'
sql_list = [
    'select distinct l_shipmode from lineitem where l_quantity <= 11.0',
    'select distinct l_shipmode from lineitem where l_quantity <= 21.0',
    'select distinct l_shipmode from lineitem where l_quantity <= 31.0',
    'select distinct l_shipmode from lineitem where l_quantity <= 41.0',
    'select distinct l_shipmode from lineitem where l_quantity <= 50.0',
]

mark_sql_list[mark] = sql_list

mark = 'SSA'
sql_list = [
    'select avg(l_quantity) from lineitem where l_quantity <= 11.0',
    'select avg(l_quantity) from lineitem where l_quantity <= 21.0',
    'select avg(l_quantity) from lineitem where l_quantity <= 31.0',
    'select avg(l_quantity) from lineitem where l_quantity <= 41.0',
    'select avg(l_quantity) from lineitem where l_quantity <= 50.0',
]
mark_sql_list[mark] = sql_list

mark = 'JS'
sql_list = [
    'select l_shipmode from lineitem where l_quantity <= 5',
    'select l_orderkey, o_orderkey, l_shipmode from lineitem, orders where l_quantity <=5 and l_orderkey = o_orderkey',
    'select l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, l_shipmode from lineitem, orders, customer, supplier where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey',
    'select l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey, l_shipmode from lineitem, orders, customer, supplier, nation, region where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey ',
    'select l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey,  p_partkey, ps_partkey, ps_suppkey, l_shipmode from lineitem, orders, customer, supplier, nation, region, part, partsupp where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey and p_partkey = ps_partkey  and s_suppkey = ps_suppkey',
]
mark_sql_list[mark] = sql_list

mark = 'JSP'
sql_list = [
    'select distinct l_shipmode from lineitem where l_quantity <= 5',
    'select distinct l_orderkey, o_orderkey, l_shipmode from lineitem, orders where l_quantity <=5 and l_orderkey = o_orderkey',
    'select distinct l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, l_shipmode from lineitem, orders, customer, supplier where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey',
    'select distinct l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey, l_shipmode from lineitem, orders, customer, supplier, nation, region where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey ',
    'select distinct l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey,  p_partkey, ps_partkey, ps_suppkey, l_shipmode from lineitem, orders, customer, supplier, nation, region, part, partsupp where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey and p_partkey = ps_partkey  and s_suppkey = ps_suppkey',
]
mark_sql_list[mark] = sql_list


mark = 'JSA'
sql_list = [
    'select l_shipmode, avg(l_quantity) from lineitem where l_quantity <= 5 group by l_shipmode',
    'select l_orderkey, o_orderkey, l_shipmode, avg(l_quantity)  from lineitem, orders where l_quantity <=5 and l_orderkey = o_orderkey group by l_orderkey, o_orderkey, l_shipmode',
    'select l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, l_shipmode, avg(l_quantity)  from lineitem, orders, customer, supplier where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey group by l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, l_shipmode,',
    'select l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey, l_shipmode, avg(l_quantity)  from lineitem, orders, customer, supplier, nation, region where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey group by l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey, l_shipmode',
    'select l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey,  p_partkey, ps_partkey, ps_suppkey, l_shipmode, avg(l_quantity)  from lineitem, orders, customer, supplier, nation, region, part, partsupp where l_quantity <=5 and l_orderkey = o_orderkey and c_custkey = o_custkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey and p_partkey = ps_partkey  and s_suppkey = ps_suppkey group by l_orderkey, o_orderkey, c_custkey, o_custkey, l_suppkey, s_suppkey, c_nationkey, s_nationkey, n_nationkey, r_regionkey, n_regionkey, r_regionkey,  p_partkey, ps_partkey, ps_suppkey, l_shipmode',
]
mark_sql_list[mark] = sql_list




mark = 'SAV1'
sql_list = [
    'select count(*) from orders',
    'select max(O_TOTALPRICE) from orders',
    'select min(O_TOTALPRICE) from orders',
    'select avg(O_TOTALPRICE) from orders',
    ]
mark_sql_list[mark] = sql_list

mark = 'SAV2'
sql_list = [
    'select O_TOTALPRICE from orders',
    ]
mark_sql_list[mark] = sql_list

# mark = 'SPV1'
# sql_list = []
# for table in table_list:
    # for field in table_fields[table]:
        # if(field != 'aID'):
            # sql_list.append(f"select {field} from {table}")
# mark_sql_list[mark] = sql_list