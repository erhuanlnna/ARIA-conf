from test_queries import *
from PVPricer import *
from MyPricer import *
from QAPricer import *
def write_strings_to_file(strings, filename):
    try:
        # Open the file in 'w' mode to write
        with open(filename, 'w') as file:
            # Iterate through the list of strings
            for string in strings:
                # Write each string to the file
                file.write(string + '\n')  # Add a newline after each string
        print("Strings have been written to", filename)
    except IOError:
        print("Error: Unable to write to the file", filename)


for mark in mark_sql_list.keys():
    print(mark, len(mark_sql_list[mark]))

pv_pricer= PVPricer(db, table_size_list, tuple_price_list, table_fields)
support_suffix = "_qa_support"
qa_pricer = QAPricer(db, table_list, table_fields, history, table_price_list, table_size_list, support_suffix, history_aware)
support_suffix = ""
# support_suffix = "_ar_support_10"
support_size_list = table_size_list
my_pricer = Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)

pricer_list = [pv_pricer, my_pricer, qa_pricer]
name_list = ["PVPricer", f"MyPricer{support_suffix}", "QAPricer"]
repeat_num = 5
for i, pricer in enumerate(pricer_list):
    all_pre_sqls = []
    for mark in mark_sql_list.keys():
        sql_list = mark_sql_list[mark]
        if("A" in mark and isinstance(pricer, PVPricer)):
            continue 
        
        new_sql_list = pricer.print_required_query(sql_list, mark)
    
        for l in new_sql_list:
            if(isinstance(pricer, PVPricer)):
                all_pre_sqls.append(l)
            elif("ar_support" in name_list[i]):
                for s in l[1:]:
                    all_pre_sqls.append(s)
            else:
                for s in l:
                    all_pre_sqls.append(s)
        
        all_pre_sqls.append("")    
    file_name = f"pre_sql/{db}-all-{name_list[i]}.txt"  
    write_strings_to_file(all_pre_sqls, file_name)  
    
query_time = []
my_query_time = []
for mark in mark_sql_list.keys():  
    sql_list = mark_sql_list[mark]
    for i, sql in enumerate(sql_list):
        query_time.append("SET @@profiling = 0;")
        query_time.append("SET @@profiling_history_size = 0;")
        query_time.append("SET @@profiling_history_size = 1000; ")
        query_time.append("SET @@profiling = 1;")
        sql = sql.split("group")[0]
        sql = sql.split("limit")[0]
        sql = sql.split("from")[1]
        sql = f"select count(*) from {sql}"
        if(";" not in sql):
            sql = sql + ";"
        for _ in range(repeat_num):
            query_time.append(sql)
        query_time.append("SET @@profiling = 0;")
        query_time.append(f"SELECT SUM(DURATION)/{repeat_num} FROM INFORMATION_SCHEMA.PROFILING INTO OUTFILE '/var/lib/mysql-files/{db}-{mark}-{i}-time.csv';")
        query_time.append("")

        
        sql = sql_list[i]
        new_sql_list = qa_pricer.print_required_query([sql], mark)
        new_sql_list = new_sql_list[0]
        # obtain the additional corresponding sql
        new_sql_list = new_sql_list[1:]
        if(len(new_sql_list) != 0):
            query_time.append("SET @@profiling = 0;")
            query_time.append("SET @@profiling_history_size = 0;")
            query_time.append("SET @@profiling_history_size = 1000; ")
            query_time.append("SET @@profiling = 1;")
            for sql in new_sql_list:
                sql = sql.split("group")[0]
                sql = sql.split("limit")[0]
                sql = sql.split("INTO")[0]
                sql = sql.split("from")[1]
                sql = f"select count(*) from {sql}"
                if(";" not in sql):
                    sql = sql + ";"
                for _ in range(repeat_num):
                    query_time.append(sql)    
            query_time.append("SET @@profiling = 0;")
            query_time.append(f"SELECT SUM(DURATION)/{repeat_num} FROM INFORMATION_SCHEMA.PROFILING INTO OUTFILE '/var/lib/mysql-files/{db}-{mark}-{i}-{name_list[2]}.csv';")
            query_time.append("")
                
        sql = sql_list[i]
        new_sql_list = my_pricer.print_required_query([sql], mark)
        new_sql_list = new_sql_list[0]
        # obtain the additional corresponding sql
        new_sql_list = new_sql_list[1:]
        if len(new_sql_list) != 0:
            my_query_time.append("SET @@profiling = 0;")
            my_query_time.append("SET @@profiling_history_size = 0;")
            my_query_time.append("SET @@profiling_history_size = 1000; ")
            my_query_time.append("SET @@profiling = 1;")
            for sql in new_sql_list:
                sql = sql.split("group")[0]
                sql = sql.split("limit")[0]
                sql = sql.split("INTO")[0]
                sql = sql.split("from")[1]
                sql = f"select count(*) from {sql}"
                if(";" not in sql):
                    sql = sql + ";"
                for _ in range(repeat_num):
                    my_query_time.append(sql)    
            my_query_time.append("SET @@profiling = 0;")
            my_query_time.append(f"SELECT SUM(DURATION)/{repeat_num} FROM INFORMATION_SCHEMA.PROFILING INTO OUTFILE '/var/lib/mysql-files/{db}-{mark}-{i}-{name_list[1]}.csv';")
            my_query_time.append("")
        
        
file_name = f"pre_sql/{db}-time-query.txt"    
write_strings_to_file(query_time, file_name)      
file_name = f"pre_sql/{db}-time-query{support_suffix}.txt"  
write_strings_to_file(my_query_time, file_name)   