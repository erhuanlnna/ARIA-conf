from parse_sql import QueryMetaData
import bisect
import copy
from pandas.errors import EmptyDataError
from test_queries import *
from functools import reduce
from collections import _count_elements, Counter
import re


def compute_removed_uncertainty_avg(s_values, real_values, support_size):
    n = len(real_values)
    if(n == 1):
        if(len(s_values) == n):
            return support_size - 1
        else:
            cnt = sum([s == real_values[0] for s in s_values])
            return support_size - cnt
    if(n == 2):
        if(len(s_values) == n):
            return support_size * n - 1 - 1
        avg_value = (real_values[0]+ real_values[1])/2 
        # the first tuple [x_l, avg], the second tuple [avg, x_h]
        x_h = max(real_values)
        x_l = min(real_values)
        cnt1 = cnt2 = 0 
        for s in s_values:
            if(s == avg_value):
                cnt1 += 1
                cnt2 += 1
            elif(s >= x_l):
                cnt1 += 1
            elif(s <= x_h):
                cnt2 += 1 
        return support_size * n - cnt1 - cnt2
        
    sum_value = float(np.sum(real_values))
    x_h = max(real_values)
    x_l = min(real_values)
    s_values = np.sort(s_values)
    tmp_a = np.fromfunction(lambda i: (sum_value - (n - i - 1) * x_h)/(i + 1), (n, ), dtype=float)
    tmp_b = np.fromfunction(lambda i: (sum_value - (i) * x_l)/(n - i), (n, ), dtype=float)
    max_a = np.ones(n) * x_l 
    min_b = np.ones(n) * x_h 
    a_i_list = np.maximum.reduce([tmp_a, max_a])
    b_i_list = np.minimum.reduce([tmp_b, min_b])
    distinct_a_i_list = np.unique(a_i_list)
    distinct_b_i_list = np.unique(b_i_list)
    min_right_idx = bisect.bisect_right(s_values, sum_value/n)
    max_left_idx = bisect.bisect_left(s_values[:min_right_idx], sum_value/n)

    left_idx_dict = vectorized_search(s_values[:min_right_idx + 1], distinct_a_i_list, 'left')
    right_idx_dict = vectorized_search(s_values[max_left_idx:], distinct_b_i_list, 'right')
    left_idx_dict[a_i_list[0]] = 0
    right_idx_dict[b_i_list[-1]] = len(s_values[max_left_idx:])

    right_idxs = np.array([right_idx_dict[x] for x in b_i_list])
    left_idxs = np.array([left_idx_dict[x] for x in a_i_list])
    cnt_list = right_idxs - left_idxs + max_left_idx
    # cnt_list = [right_idx_dict[b_i_list[i]] - left_idx_dict[a_i_list[i]] + max_left_idx for i in range(n)]
    tmp_price = support_size * n - np.sum(cnt_list)
    return tmp_price



def vectorized_search(data, higher_list, side):
    indices = np.searchsorted(data, higher_list, side= side)
    return dict(zip(higher_list, indices))



def read_data_from_file(filename):
    data = []  # Initialize an empty list to store the data
    with open(filename, 'r') as file:
        for line in file:
            # Split the line by commas and strip any leading/trailing whitespace from each element
            line_data = [element.strip() for element in line.split(',')]
            data.append(line_data)  # Append the list to the main data container
    return data

def havok_method(tests):
    def reducer(accumulator, element):
        for key, value in element.items():
            accumulator[key] = accumulator.get(key, 0) + value
        return accumulator
    return reduce(reducer, tests, {})

def count(iterable):
    """Explicit iteration over items."""
    dctCounter = {}
    for item in iterable:
        if item in dctCounter: 
            dctCounter[item] += 1
        else: 
            dctCounter[item]  = 1
    return dctCounter

def count2(iterable):
    """Iterating over the indices"""
    dctCounter = {}
    lenLstItems = len(iterable)
    for idx in range(lenLstItems):
        item = iterable[idx]
        if item in dctCounter.keys(): 
            dctCounter[item] += 1
        else: 
            dctCounter[item]  = 1
    return dctCounter

def c_count(iterable):
    """Internal counting function that's used by Counter"""
    d = {}
    _count_elements(d, iterable)
    return d

def load_pre_query_results(sql, mark, i, db, support_suffix):
    try:
        df = pd.read_csv(f'pre_rs/{db}-{mark}-{i}-MyPricer-o.txt', header=None, na_values=['\\N'])
        all_results = df.values
    except EmptyDataError:
        print(f"No columns to parse from file pre_rs/{db}-{mark}-{i}-MyPricer-o.txt")
        all_results = []

    table_list = parse_sql_statements(sql)
    support_rs = []
    if(support_suffix != ""):
        for ii in range(len(table_list)):
            try:
                df = pd.read_csv(f'pre_rs/{db}-{mark}-{i}-MyPricer-{ii}-{support_suffix}.txt', header=None, na_values=['\\N'])
                tmp_v = df.values
            except EmptyDataError:
                print(f"No columns to parse from file pre_rs/{db}-{mark}-{i}-MyPricer-{ii}-{support_suffix}.txt")
                tmp_v = []
            support_rs.append(tmp_v)
    return all_results, support_rs
def parse_sql_statements(sql_statement : str):
    if 'where' in sql_statement:
        rule_projections = r'from(.*?)where'
    elif("group" in sql_statement):
        rule_projections = r'from(.*?)group'
    elif("limit" in sql_statement):
        rule_projections = r'from(.*?)limit'
    else:
        rule_projections = r'(?<=from).*$'
    table_list = re.findall(rule_projections, sql_statement)

    table_list = table_list[0].replace(' ','').split(',') 
    if 'distinct' in sql_statement:
        rule_selections = r'distinct(.*?)from'
    else:
        rule_selections = r'select(.*?)from'

    return table_list


class Pricer:
    def __init__(self, db, table_size_list, support_size_list, price_history, tuple_price_list, table_fields, field_domin, support_suffix, history_aware):
        self.db = db
        self.table_fields = table_fields
        self.table_size_list = table_size_list
        self.price_history = price_history
        self.support_size_list = support_size_list
        self.tuple_price_list = tuple_price_list
        self.field_domin = field_domin
        self.support_suffix = support_suffix
        self.history_aware = history_aware 


    def __pre_price_cnt_query__(self, sql, o_results, s_results_list):
        query_tables = parse_sql_statements(sql)
        if(len(query_tables) == 1 and "group by" not in sql):
            price = 0
            table = query_tables[0]
            tuple_price = self.tuple_price_list[table]
            table_size = self.table_size_list[table]
            support_size = self.support_size_list[table]
            s_results = [[0]]
            if(len(s_results_list) != 0):
                s_results = s_results_list[0]
            n = o_results[0][0]
            E_n = s_results[0][0] + n
            E_a = E_n
            tmp_price = support_size * n - n * E_n 
            price += tmp_price/(support_size - 1) * tuple_price
            tmp_price = (table_size - n) * E_a 
            price += tmp_price/(support_size - 1) * tuple_price
            return price
        else:
            sql = sql.split("group")[0]
            tmp_str = sql.split("from")
            tmp_str1 = tmp_str[0]
            tmp_str2 = tmp_str[1]
            id_select = tmp_str1.split(",")[:-1]
            tmp_str1 = ",".join(id_select)
            new_sql = tmp_str1 + " from " + tmp_str2
            # pricing as normal
            price = self.__pre_price_SPJA_query__(new_sql, o_results, s_results_list)
            return price

    def __pre_price_SPJ_star__(self, sql, o_results, s_results_list):
        is_distinct = "distinct" in sql
        k = -1
        sql = sql.replace("distinct", "")
        tmp_sql = sql.split("limit")
        sql = tmp_sql[0]
        if(len(tmp_sql) > 1):
            k = int(tmp_sql[1])
            if(k == 0):
                return 0
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            table_size = self.table_size_list[table]
            tuple_price = self.tuple_price_list[table]  
            if(is_distinct):
                # get the number of distinct results 
                lines = o_results[:, 0]
                lines = set(lines)
                n = len(lines)
                E_n = n * 1
                E_a = 0
                if(len(s_results_list) != 0):
                    s_results = s_results_list[0]
                    E_a =  s_results[0][0]
            else:
                s_results = [[0]]
                if(k == -1):
                    k = o_results[0][0] + 1
                
                if(len(s_results_list) != 0):
                    s_results = s_results_list[0]
                    

                n = min(k, o_results[0][0])
                E_n = n * 1
                if(k > o_results[0][0]):
                    E_a = s_results[0][0] + o_results[0][0]
                else:
                    E_a = 0

            
            price = support_size * n  - E_n
            price += (table_size - n) * (E_a)
            price = price / (support_size - 1) * tuple_price
            price_list.append(price)
        else:
            if(is_distinct):
                selected_idx = [len(self.table_fields[table]) for table in query_tables]
            else:
                selected_idx = [1 for table in query_tables]
            o_results = np.array(o_results)
            tmp_n = len(o_results)
            if(k == -1):
                k = tmp_n + 1
            
            for i, table in enumerate(query_tables):
                price = 0
                o_1 = []
                o_2 = []
                support_size = self.support_size_list[table]
                table_size = self.table_size_list[table]
                tuple_price = self.tuple_price_list[table]
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                # n = len(o_1)
                if(k >= tmp_n):
                    k_1 = o_1 
                else:
                    k_1 = o_results[:k, pre_idx:pre_idx+idx]
                    # remove duplicate
                    k_1 = np.unique(k_1, axis = 0)
                    
                if(len(s_results_list) != 0):
                    s_results = s_results_list[i]
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                        
                n = len(k_1)
                E_n = n * 1   
                if(is_distinct):  
                    E_a = len(o_2)
                else: 
                    if(k > tmp_n):
                        E_a = len(o_2) + len(o_1)
                    else:
                        E_a = 0
                price = support_size * n  - E_n
                price += (table_size - n) * (E_a)
                price = price / (support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)

    def __pre_price_SP_query__(self, o_results, s_results, is_distinct, k_results, table_size, support_size, tuple_price):
        # group by the results based on the selected attributes
        o_query_groups = c_count(o_results)
        if(len(s_results) == 0):
            query_groups = o_query_groups
        else:
            query_groups = c_count(s_results)
            query_groups = havok_method([o_query_groups, query_groups])    
        if(is_distinct):
            effective_states = [query_groups[item] for item in o_query_groups.keys()]
            E_n = sum(effective_states)
            E_a = len(s_results) + len(o_results) - E_n
            n = len(o_query_groups.keys())    
        else:
            effective_states = [query_groups[item] for item in k_results]
            E_n = sum(effective_states)
            n = len(k_results)
            if(n > len(o_results)):
                E_a = len(s_results) + len(o_results)
            else:
                E_a = 0
        price = support_size * n  - E_n
        price += (table_size - n) * (E_a)
        price = price / (support_size - 1) * tuple_price    
        return price
  
    def __pre_price_SPJA_query__(self, sql, o_results, s_results_list): # not the count query
        query_tables = parse_sql_statements(sql)
        is_distinct = "distinct" in sql
        flag = ""
        agg_table = ""
        x_h = 0
        x_l = 0
        if("group by" in sql):
            flag += "group by"
        if("max(" in sql or "min(" in sql or "avg(" in sql or "sum(" in sql):
            if("max(" in sql):
                flag += " max"
                pattern = r"max\((.*?)\)"
            if("min(" in sql):
                flag += " min"
                pattern = r"min\((.*?)\)"
            if("avg(" in sql):
                flag += " avg"
                pattern = r"avg\((.*?)\)"
            if("sum(" in sql):
                flag += " avg"
                pattern = r"sum\((.*?)\)"
            # agg_att = re.findall(pattern, sql)[0]
            # if "." in agg_att:
                # str_list = att.split(".")
                # table = str_list[0]
                # agg_table = table
                # x_l = self.field_domin[agg_att][0]
                # x_h = self.field_domin[agg_att][1]
            # else:
                # for table in query_tables:
                    # if(agg_att in self.table_fields[table]):
                        # agg_table = table
                        # x_l = self.field_domin[table+ "." + agg_att][0]
                        # x_h = self.field_domin[table+ "." + agg_att][1]
                        # break       
      
        k = -1
        sql = sql.replace("distinct", "")
        sql = sql.replace("avg(", "(")
        sql = sql.replace("sum(", "(")
        sql = sql.replace("max(", "(")
        sql = sql.replace("min(", "(")
        sql = sql.split("group by")[0]
        tmp_sql = sql.split("limit")
        sql = tmp_sql[0]
        if(len(tmp_sql) > 1):
            k = int(tmp_sql[1])
            if(k == 0):
                return 0
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0] 
            s_results = []
            if(len(s_results_list) != 0):
                s_results = s_results_list[0]
                if(len(s_results) != 0):
                    # print(len(s_results))
                    s_results = s_results[:, 0]
            if(len(o_results) != 0):
                o_results = o_results[:, 0]
            if(flag == ""):
                if(k == -1): # not the limit query
                    k = len(o_results) + 1
                price = self.__pre_price_SP_query__(o_results, s_results, is_distinct, o_results[:k], self.table_size_list[table], self.support_size_list[table], self.tuple_price_list[table])
            else:
                o_results = list(o_results)
                s_results = list(s_results)
                
                if("avg" in flag):
                    price = self.__pre_price_avg_query__(o_results, s_results, flag, self.table_size_list[table], self.support_size_list[table], self.tuple_price_list[table])
                else:
                    # print(len(s_results), len(s_results[0]))
                    price = self.__pre_price_extreme_query__(o_results, s_results, flag, self.table_size_list[table], self.support_size_list[table], self.tuple_price_list[table])
            
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            if(k == -1): # not the limit query
                k = len(o_results) + 1
            tmp_n = len(o_results)
            for i, table in enumerate(query_tables):
                price = 0
                o_1 = []
                o_2 = []
                k_1 = []
                if(len(o_results) != 0):
                    # print(pre_idx, idx)
                    o_1 = set(o_results[:, i])
                    if(isinstance(o_results[0][i], str)):
                        o_1 = [s[s.index("|")+1:] for s in o_1]
                if(k >= tmp_n):
                    k_1 = o_1
                else:
                    k_1 = set(o_results[:k, i])
                    if(isinstance(o_results[0][i], str)):
                        k_1 = [s[s.index("|")+1:] for s in k_1]
 
                if(len(s_results_list) != 0):
                    s_results = s_results_list[i]
                    if(len(s_results) != 0):
                        o_2 = set(s_results[:, i])
                        if(isinstance(s_results[0][i], str)):
                            o_2 = [s[s.index("|")+1:] for s in o_2]
                # group by the query results on the support set
                if("avg" in flag and table == agg_table):
                    price = self.__pre_price_avg_query__(o_1, o_2, flag, self.table_size_list[table], self.support_size_list[table], self.tuple_price_list[table])
                elif(("max" in flag or "min" in flag) and table == agg_table):
                    price = self.__pre_price_extreme_query__(o_1, o_2, flag, self.table_size_list[table], self.support_size_list[table], self.tuple_price_list[table])
                else:
                    price = self.__pre_price_SP_query__(o_1, o_2, is_distinct, k_1, self.table_size_list[table], self.support_size_list[table], self.tuple_price_list[table])
                price_list.append(price)

        return sum(price_list)

     

    def __pre_price_extreme_query__(self, o_results, s_results, flag, table_size, support_size, tuple_price):
        # flag = 1 (group-by-max) 2 (group-by-min) -1 (group-by-max) -2 (group-by-min)
        # get the max/min value
        n = len(o_results)
        if(n == 0):
            # empty results
            # results_id = [item[0] for item in s_results] + [item[0] for item in o_results]
            E_a = len(s_results) + len(o_results)
            tmp_price = E_a / (support_size - 1) * tuple_price
            price = (table_size - n) * tmp_price
            return price
        if("group by" not in flag and len(s_results) == 0):
            # sort the original results 
            # print("-------")
            # s1 = time.time()
            extreme_value = None
            if("max" in flag):
                # extreme_value = max(o_results)[-1]
                extreme_value = max(o_results)
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
            else:
                extreme_value = min(o_results)
            
            extreme_num = o_results.count(extreme_value)
            tmp_price = (support_size - extreme_num)
            price = tmp_price/(support_size - 1) * tuple_price
            # print(time.time()-s1)
            return price
            # the effective states of other tuples are the S_t 
            # have no change and no price
        if("group by" in flag and len(s_results) == 0):
            price = 0
            # group by the original results
            query_groups = defaultdict(list)
            for item in o_results:
                idx = item.rfind("|")
                if(item[idx+1:] == None or item[idx+1:] == ""): # NULL value
                    continue
                query_groups[item[:idx]].append(float(item[idx+1:]))
            for group_v in query_groups.keys():
                group_results = query_groups[group_v]
                extreme_value = None
                if("max" in flag):
                    extreme_value = max(group_results)
                    # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
                else:
                    extreme_value = min(group_results)
                    # extreme_value = min(group_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
                cnt = group_results.count(extreme_value)
                # find the tuple with the maximum price
                tmp_price = (support_size - cnt)
                price += tmp_price/(support_size - 1) * tuple_price
                # print(group_v, cnt)
            # print(price)
            return price

        if("group by" not in flag and len(s_results) != 0):
            # do not group by
            extreme_value = None
            if("max(" in flag):
                extreme_value = max(o_results)
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
            else:
                extreme_value = min(o_results)      
            extreme_num = 0
            ineffective_num = 0
            price = 0
            for item in s_results:
                if(item is not None and item == extreme_value):
                    extreme_num += 1
                if("max" in flag):
                    if(item is None or item > extreme_value):
                        ineffective_num += 1
                else:
                    if(item is None or item < extreme_value):
                        ineffective_num += 1
            extreme_num += o_results.count(extreme_value)

            
            # find the tuple with the maximum price
            tmp_price = (support_size - extreme_num)
            price += tmp_price/(support_size - 1) * tuple_price
            # the effective states of other tuples cannot be higher than max or lower than minimum 
            E_a = ineffective_num
            price += (E_a/(support_size - 1) * tuple_price) * (table_size - 1)
            return price
        if("group by" in flag and len(s_results) != 0):
            
            E_a = len(s_results) + len(o_results)
            effective_id_list = []
            price = 0
            # group by the original results 
            o_query_groups = defaultdict(list)
            s_query_groups = defaultdict(list)
            for item in o_results:
                idx = item.rfind("|")
                if(item[idx+1:] == None or item[idx+1:] == ""): # NULL value
                    continue
                o_query_groups[item[:idx]].append(float(item[idx+1:]))
                s_query_groups[item[:idx]].append(float(item[idx+1:]))
                
            for item in s_results:
                idx = item.rfind("|")
                if(item[idx+1:] == None or item[idx+1:] == ""): # NULL value
                    continue
                # o_query_groups[item[:idx]].append(float(item[idx+1:]))
                s_query_groups[item[:idx]].append(float(item[idx+1:]))
            for group_v in o_query_groups.keys():
                group_r = o_query_groups[group_v]
                group_s = s_query_groups[group_v]
                # print(group_v, len(group_r), len(group_s))
                extreme_value = None
                extreme_num = 0
                effective_num = 0
                if("max" in flag):
                    extreme_value = max(group_r)
                    extreme_num += group_r.count(extreme_value)
                    for item in group_s:
                        # print(item[-1], extreme_value)
                        if(item is not None and (item) == extreme_value): # take the same value 
                            extreme_num += 1
                        if(item is None or item <= extreme_value): # take the effective (no higher) value 
                            effective_num += 1
                else:
                    extreme_value = min(group_r)
                    extreme_num += group_r.count(extreme_value)
                    for item in group_s:
                        if(item is not None and item == extreme_value): # take the same value 
                            extreme_num += 1
                        if(item is None or item >= extreme_value): # take the effective (no less) value 
                            effective_num += 1
                # find the tuple with the maximum price
                tmp_price = support_size - extreme_num
                price += tmp_price/(support_size - 1) * tuple_price
                # print(tmp_price)
                # print(group_v, extreme_num)
                # the effective states of other tuples are the S_t - results_id + effective_id_list 
                E_a = E_a - effective_num
            # print(price, E_a, support_size - 1)
            price += E_a/(support_size - 1) * tuple_price * (table_size - len(o_query_groups.keys()))
            # print(price)
            return price    
  
    def __pre_price_avg_query__(self, o_results, s_results, flag, table_size, support_size, tuple_price):
        n = len(o_results)
        if(n == 0):
            price = 0
            tmp_price = len(s_results) + n
            price += tmp_price/(support_size - 1) * tuple_price * table_size
            return price
        if("group by" in flag):
            # group by the results based on the selected attributes
            query_groups = defaultdict(list)
            real_groups = defaultdict(list)
            for item in o_results:
                # print(item)
                idx = item.rfind("|")
                if(item[idx+1:] == None or item[idx+1:] == ""): # NULL value
                    continue
                real_groups[item[:idx]].append(float(item[idx+1:]))
                query_groups[item[:idx]].append(float(item[idx+1:]))
            
            for item in s_results:
                idx = item.rfind("|")
                if(item[idx+1:] == None or item[idx+1:] == ""): # NULL value
                    continue
                query_groups[item[:idx]].append(float(item[idx+1:]))
                
            # print("Start to price query")
            tmp_price_list = []
            price = 0
            for j, item in enumerate(real_groups.keys()):
                tmp_price = compute_removed_uncertainty_avg(query_groups[item], real_groups[item], support_size)
                tmp_price_list.append(tmp_price)
                price += tmp_price/(support_size - 1) * tuple_price
            tmp_price = len(s_results)  + len(o_results)
            price += tmp_price/(support_size - 1) * tuple_price * (table_size - len(o_results))   
            return price

        else:
            price = 0
            
            if(len(s_results) != 0):
                s_values = s_results + o_results
            else:
                s_values = o_results
            tmp_price = compute_removed_uncertainty_avg(s_values, o_results, support_size)
            price += tmp_price/(support_size - 1) * tuple_price
            tmp_price = len(s_results)   
            price += tmp_price/(support_size - 1) * tuple_price * (table_size - len(o_results))    
            return price
    
    def price_SQL_query(self, sql):
        sql = sql.split(";")[0]
        sql  = sql + " "
        sql_list = [sql]
        new_sql_list = self.print_required_query(sql_list)
        s = new_sql_list[0].split("INTO")[0]
        o_results = select(s, database= self.db)
        s_results = []
        if(self.support_suffix != ""):
            for s in new_sql_list[1:]:
                s = s.split("INTO")[0]
                s_results.append(select(s, database= self.db))
        
        price = self.pre_price_SQL_query(sql, o_results, s_results)
        return price
    

    def pre_price_SQL_query(self, sql, o_results, s_results):
        sql = sql.split(";")[0]
        sql  = sql + " "

        if("count(" in sql):
            price = self.__pre_price_cnt_query__(sql, o_results, s_results)
        elif("*" in sql):
            price = self.__pre_price_SPJ_star__(sql, o_results, s_results)
        else:
            price = self.__pre_price_SPJA_query__(sql, o_results, s_results)
        return price
    def print_required_query(self, sql_list, mark = ""):
        new_sql_list = []
        mark_sql_list = []
        current_directory =  '/var/lib/mysql-files'
        for i, sql in enumerate(sql_list):
            new_sql_list.append([])
            sql = sql.split(";")[0]
            sql  = sql + " "
            query_tables = parse_sql_statements(sql)
            table_num = len(query_tables)
            is_distinct = "distinct" in sql
            if(mark == ""):
                if(is_distinct):
                    mark = "SP"
                elif("count(" in sql or "max(" in sql or "min(" in sql or "avg(" in sql or "sum(" in sql):
                    mark = "SA"
                else:
                    mark = "S"
                if(table_num > 1):
                    mark += "J"
            mark_sql_list.append(mark)
            
            sql = sql.replace("distinct", "")
            sql = sql.split("limit")[0]
            sql = sql.split("group")[0]
            if("count(" in sql):
                tmp_str_list =sql.split("from")
                tmp_str2 = tmp_str_list[0].split(",")
                if(len(tmp_str2) > 1):
                    # remove the last count(*) clause
                    tmp_str = ", ".join(tmp_str2[:-1])
                else:
                    tmp_str = ", ".join(tmp_str2)
                sql = f"{tmp_str} from {tmp_str_list[1]}"
            
            flag = agg_table = ""
            if("max(" in sql or "min(" in sql or "avg(" in sql or "sum(" in sql):
                if("max(" in sql):
                    flag += " max"
                    pattern = r"max\((.*?)\)"
                if("min(" in sql):
                    flag += " min"
                    pattern = r"min\((.*?)\)"
                if("avg(" in sql):
                    flag += " avg"
                    pattern = r"avg\((.*?)\)"
                if("sum(" in sql):
                    flag += " avg"
                    pattern = r"sum\((.*?)\)"
                agg_att = re.findall(pattern, sql)[0]
                if "." in agg_att:
                    str_list = att.split(".")
                    table = str_list[0]
                    agg_table = table
                else:
                    for table in query_tables:
                        if(agg_att in self.table_fields[table]):
                            agg_table = table
                            break 
            
            sql = sql.replace("max(", "(")
            sql = sql.replace("min(", "(")
            sql = sql.replace("avg(", "(")
            sql = sql.replace("sum(", "(")         
            if(table_num == 1):
                tmp_sql = sql + ""
                if("*" in sql and "count(" not in sql):
                    sql = sql.replace("*", "count(*)")
                    tmp_sql = sql + ""
                    if(is_distinct):
                        str_list = []
                        for table in query_tables:
                            for s in self.table_fields[table]:
                                str_list.append(table+"."+ s)
                        place_str = ",".join(str_list)
                        tmp_sql = sql.replace("*", f"CONCAT_WS('|', {place_str})")  
                elif("count(" not in sql):
                # elif("A" not in mark): # i.e.,  S, SP, SJ, SPJ, queries
                    # "count(" not in tmp_sql
                    str = tmp_sql.split("select")[1]
                    str = str.split("from")

                    str1 = str[0]
                    str2 = str[1]
                    tmp_sql = f"select CONCAT_WS('|', {str1}) from {str2}"
                    sql = tmp_sql + ""
                          
                outfile_path = current_directory + f"/{self.db}-{mark_sql_list[-1]}-{i}-MyPricer-o.txt"
                tmp_sql = f"{tmp_sql} INTO OUTFILE '{outfile_path}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n';"
                new_sql_list[-1].append(tmp_sql)
                if(self.support_suffix != ""):
                    table = query_tables[0]
                    ii = 0
                    new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    outfile_path = current_directory + f"/{self.db}-{mark_sql_list[-1]}-{i}-MyPricer-{ii}-{self.support_suffix}.txt"
                    new_sql = f"{new_sql} INTO OUTFILE '{outfile_path}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n';"
                    new_sql_list[-1].append(new_sql)
            else:
                str1 = sql.split("from")[1]
                selected_attributes = {table: [table + ".aID"] for table in query_tables}
                # print(sql)
                md = QueryMetaData(sql)
                if("*" not in md.projections):
                    for att in md.projections:
                        if "." in att:
                            str_list = att.split(".")
                            table = str_list[0]
                            selected_attributes[table].append(att)
                        else:
                            for tt in query_tables:
                                if(att in self.table_fields[tt]):
                                    selected_attributes[tt].append(att)
                                    break
                    
                if(is_distinct and "*" in md.projections): # distinct *
                    for table in query_tables:
                        for att in self.table_fields[table]:
                            selected_attributes[table].append(f"{table}.{att}")
                
                if(agg_table != ""):
                    selected_attributes[table].append(f"{agg_att}")
                str_list = []
                for table in query_tables:
                    str_list.append("CONCAT_WS('|'," + ", ".join(selected_attributes[table]) + ")")
                place_str = ",".join(str_list)
                tmp_sql = "select " + place_str + " from " + str1

                outfile_path = current_directory + f"/{self.db}-{mark_sql_list[-1]}-{i}-MyPricer-o.txt"
                tmp_sql = f"{tmp_sql} INTO OUTFILE '{outfile_path}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n';"
                new_sql_list[-1].append(tmp_sql)
                if(self.support_suffix != ""):
                    for ii, table in enumerate(query_tables):
                        new_sql = "select " + str_list[ii] + " from " + str1
                        new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                        new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                        new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                        outfile_path = current_directory + f"/{self.db}-{mark_sql_list[-1]}-{i}-MyPricer-{ii}-{self.support_suffix}.txt"
                        new_sql = f"{new_sql} INTO OUTFILE '{outfile_path}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n';"
                        new_sql_list[-1].append(new_sql)

                
        return new_sql_list        
if __name__ == "__main__":

    print("-----------------------")
    # initiliaze the ARIA pricer
    # _ar_support_2
    # support_suffix = "_ar_support_2"
    support_suffix = ""
    support_size_list = table_size_list
    my_pricer = Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)

    for mark in mark_sql_list.keys():
        if("J" in mark or "A" in mark):
            continue
        sql_list = mark_sql_list[mark]
        # my_pricer.print_required_query(sql_list)
        for i, sql in enumerate(sql_list):
            # print(mark, i, sql)
            all_results, support_rs = load_pre_query_results(sql, mark, i, db, support_suffix)
            my_pricer.pre_price_SQL_query(sql, all_results, support_rs)
        
                            



