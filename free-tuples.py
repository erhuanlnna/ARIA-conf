from QAPricer import *
from sqlalchemy import create_engine
from test_queries import db
if __name__ == "__main__":

    table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
    tuple_price = 1
    table_price_list = {}
    support_suffix = "_qa_support"
    history_aware = False
    history = {}
    for table in table_list:
        history[table] = []
        table_price_list[table] = table_size_list[table] * tuple_price
    pricer = QAPricer(db, table_list, table_fields, history, table_price_list, table_size_list, support_suffix, history_aware)
    missing_tuples_num = 0
    db_size = 0
    for table in table_list:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@localhost/{db}')
        query = f"SELECT aID, name FROM {table}" 
        # Read the data into a DataFrame
        df = pd.read_sql_query(query, engine)
        
        
        # all_tuples = set([i+1 for i in range(table_size_list[table])])
        no_free_tuples = []
        db_size += table_size_list[table]
        support_sets = pricer.support_sets[table]
        support_size = len(support_sets)
        print(table, support_size)
        for i in range(support_size):
            support = support_sets[i]
            aid = support[1]
            bid = support[2]
            no_free_tuples.append(aid)
            no_free_tuples.append(bid)
        no_free_tuples = set(no_free_tuples)
            
        
        
        place_str = ",".join(table_fields[table])
        query = f"SELECT distinct name FROM {table}_qa_support" 
        # Read the data into a DataFrame
        df2 = pd.read_sql_query(query, engine)
        column_values = df2.values
        for aid, name in df.values:
            if(aid not in no_free_tuples):
                if(name in column_values):
                    no_free_tuples.add(aid)
                # test the query 
                # sql = f"select {place_str} from {table} where name = '{name}'"
                # rs1 = pd.read_sql_query(sql, engine)
                # sql = f"select {place_str} from {table}_qa_support where name = '{name}'"
                # rs2 = pd.read_sql_query(sql, engine)
                # if(len(rs2) != 0):
                    # no_free_tuples.add(aid)
                    # # if(not rs1.equals(rs2)):
                        # # no_free_tuples.add(aid)
                # may other tuples having the same value on the attribute can be outputted 
                

            
        
        missing_tuples_num += table_size_list[table]- len(no_free_tuples)
    
    print(missing_tuples_num, db_size, missing_tuples_num/db_size)
