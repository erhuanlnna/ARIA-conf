
from test_queries import *
import pulp
import cplex
from pandas.errors import EmptyDataError
from ortools.linear_solver import pywraplp

def load_pre_query_results(sql, mark, i, db):
    try:
        df = pd.read_csv(f'pre_rs/{db}-{mark}-{i}-PVPricer-o.txt', header=None, na_values=['\\N'])
        # all_results = df.values
        all_results = list(df.itertuples(index=False, name=None))
    except EmptyDataError:
        print(f"No columns to parse from file pre_rs/{db}-{mark}-{i}-PVPricer-o.txt")
        all_results = []
    return all_results
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
    # print('11',table_list)
    table_list = table_list[0].replace(' ','').split(',') 
    if 'distinct' in sql_statement:
        rule_selections = r'distinct(.*?)from'
    else:
        rule_selections = r'select(.*?)from'

    return table_list
class lineage:
    def __init__(self, table_num, lineage_set, is_copy = True):
        self.table_num = table_num
        self.lineage = [set() for i in range(table_num)]
        if(len(lineage_set) != 0 and is_copy):
            for i in range(table_num):
                self.lineage[i] = lineage_set[i].copy()
        if(len(lineage_set) != 0 and not is_copy):
            for i in range(table_num):
                self.lineage[i] = lineage_set[i]
    def add(self, tuple_lineage_set):
        for i in range(self.table_num):
            self.lineage[i].add(tuple_lineage_set[i])
    

    def final(self):
        for i in range(self.table_num):
            self.lineage[i] = set(self.lineage[i])

    def get_price(self, table_price_list):
        # self.final()
        price = 0
        for i in range(self.table_num):
            price += len(self.lineage[i]) * table_price_list[i]
        return price

class VPricer:
    def __init__(self, db, table_size_list, tuple_price_list):
        self.db = db
        self.table_size_list = table_size_list
        self.tuple_price_list = tuple_price_list




    
    
    def __price_distinct_query__(self, sql):
        sql = sql.replace("distinct", "")
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables

        if("*" in sql):
            place_str = ""
            str_list = []
            for table in query_tables:
                # print(table)
                for s in self.table_fields[table]:
                    str_list.append(table+"."+ s)
            place_str = ",".join(str_list)
            sql = sql.replace("*", place_str)
        
        str1 = sql.split("select")[1]
        selected_attributes = [table + ".aID" for table in query_tables]
        str2 = ",".join(selected_attributes)
        new_sql = "select " + str2 + ", " + str1
        # print(new_sql)
        table_num = len(query_tables)
        o_results = select(new_sql, database= self.db)
        o_results = np.array(o_results)
        
        variable_list = []
        all_variable_list = []
        cost_list = {}
        variable_table = defaultdict(set)
        variables = {}
            
        # get the lineage set of each query result
        tuple_lineage_set = defaultdict(list)
        for item in o_results:
            aID_list = item[:table_num]
            tuple_lineage_set[tuple(item[table_num:])].append(aID_list)
            for j, table in enumerate(query_tables):
                variable_table[table].add(aID_list[j])
        
        
        solver = pywraplp.Solver.CreateSolver('SCIP')  
        if not solver:
            raise Exception("Cannot create the solver")
    
        
        
        for i, item in enumerate(o_results):
            aID_list = item[:table_num]
            for j, table in enumerate(query_tables):
                variable_table[table].add(aID_list[j])
        
        # Create variables
        variables = {}
        for table in query_tables:
            for i in variable_table[table]:
                var_name = f"{table}_{i}"
                variables[var_name] = solver.BoolVar(var_name)
                variable_list.append(var_name)
                all_variable_list.append(var_name)
                cost_list[var_name] = self.tuple_price_list[table]
        
        i = 0
        for item in tuple_lineage_set.keys():
            for _ in range(len(tuple_lineage_set[item])):
                var_name = f"result_{i}"
                variables[var_name] = solver.BoolVar(var_name)
                all_variable_list.append(var_name)
                i += 1
        
        # Set the objective function
        objective = solver.Objective()
        for var in variable_list:
            objective.SetCoefficient(variables[var], cost_list[var])
        objective.SetMinimization()
        
        # Add constraints
        i = 0
        for item in tuple_lineage_set.keys():
            last_i = i
            for aID_list in tuple_lineage_set[item]:
                for j, table in enumerate(query_tables):
                    var = f"{table}_{aID_list[j]}"
                    var2 = f"result_{i}"
                    solver.Add(variables[var] >= variables[var2])
                i += 1
            solver.Add(solver.Sum([variables[f"result_{k}"] for k in range(last_i, i)]) >= 1)
        
        # Solve the problem
        status = solver.Solve()
        
        if status == pywraplp.Solver.OPTIMAL:
            # print("Found the solution")
            price = solver.Objective().Value()
            # print(f"Total price: {price}")
            # for var_name in all_variable_list:
                # print(f"{var_name} = {variables[var_name].solution_value()}")
        else:
            price = -1
            # print("NotFound the solution")    
        return price

    def __price_normal_query__(self, sql): # limit query, * query
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        
        selected_attributes = [table + ".aID" for table in query_tables]
        str2 = ",".join(selected_attributes)
        str1 = sql.split("from")[1]
        new_sql = "select " + str2 + " from " + str1
        # print(new_sql)
        table_num = len(query_tables)
        o_results = select(new_sql, database= self.db)
        o_results = np.array(o_results)
        # print(self.table_size_list)
        # construct the ILP problem
        
        solver = pywraplp.Solver.CreateSolver('SCIP')  
        if not solver:
            raise Exception("Cannot create the solver")
        
        variable_list = []
        cost_list = {}
        variables = {}
        variable_table = defaultdict(set)
        
        for i, item in enumerate(o_results):
                aID_list = item[:table_num]
                for j, table in enumerate(query_tables):
                    variable_table[table].add(aID_list[j])
        
        for table in query_tables:
            for i in variable_table[table]:
                var_name = f"{table}_{i}"
                variable_list.append(f"{table}_{i}")
                variables[var_name] = solver.BoolVar(var_name)
                cost_list[f"{table}_{i}"] = self.tuple_price_list[table]

        objective = solver.Objective()
        for var in variable_list:
            objective.SetCoefficient(variables[var], cost_list[var])
        objective.SetMinimization()

        
        for i, item in enumerate(o_results):
            aID_list = item[:table_num]
            for j, table in enumerate(query_tables):
                var = f"{table}_{aID_list[j]}"
                solver.Add(variables[var] >= 1)
                
        # Solve the problem
        status = solver.Solve()
        
        if status == pywraplp.Solver.OPTIMAL:
            # print("Found the solution")
            price = solver.Objective().Value()
            # print(f"Total price: {price}")
            # for var_name in all_variable_list:
                # print(f"{var_name} = {variables[var_name].solution_value()}")
        else:
            price = -1
            # print("NotFound the solution")  

        return price

                
 
    def print_required_query(self, sql_list, mark):
        new_sql_list = []
        # current_directory = os.getcwd()
        current_directory =  '/var/lib/mysql-files/'
        for i, sql in enumerate(sql_list):
            sql = sql.split(";")[0]
            sql  = sql + " "
            new_sql = ""
            if("distinct" in sql):
                sql = sql.replace("distinct", "")
                sql = sql.replace("*", "")
                md = QueryMetaData(sql)
                # print(md)
                query_tables = md.tables
                
                str1 = sql.split("from")[1]
                selected_attributes = [table + ".aID" for table in query_tables]
                str2 = ",".join(selected_attributes)
                new_sql = "select " + str2 + " from " + str1
            elif("count(" in sql or "max(" in sql or "min(" in sql or "avg(" in sql or "sum(" in sql):
                print("Provenance-based methods do not support aggregate queries!")
            else:
                md = QueryMetaData(sql)
                # print(md)
                query_tables = md.tables
                
                selected_attributes = [table + ".aID" for table in query_tables]
                str2 = ",".join(selected_attributes)
                if("*" in sql):
                    str1 = sql.split("*")[1]
                    new_sql = "select " + str2 + " " + str1
                else:
                    str1 = sql.split("from")[1]
                    new_sql = "select " + str2 + " from " + str1
            

            outfile_path = current_directory + f'{self.db}-{mark}-{i}-PVPricer-o.txt'
            new_sql = f"{new_sql} INTO OUTFILE '{outfile_path}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n';"
            # print(new_sql)
            new_sql_list.append(new_sql)

        return new_sql_list
    def price_SQL_query(self, sql):
        sql = sql.split(";")[0]
        sql  = sql + " "
        if("distinct" in sql):
            price = self.__price_distinct_query__(sql)

        elif("count(" in sql or "max(" in sql or "min(" in sql or "avg(" in sql or "sum(" in sql):
            print("Provenance-based methods do not support aggregate queries!")
            price = -1
        else:
            price = self.__price_normal_query__(sql)
        return price
    

    def pre_price_SQL_query(self, is_distinct, o_results, query_tables):
        # get the lineage set of each query result
        tuple_lineage_set = defaultdict(list)
        table_num = len(query_tables)
        if(not is_distinct):
            # construct the ILP problem
            variable_list = []
            cost_list = {}
            variable_table = defaultdict(set)
            variables = {}
            for i, item in enumerate(o_results):
                aID_list = item[:table_num]
                for j, table in enumerate(query_tables):
                    variable_table[table].add(aID_list[j])
                    
            solver = pywraplp.Solver.CreateSolver('SCIP')  
            if not solver:
                raise Exception("Cannot create the solver")
            for table in query_tables:
                for i in variable_table[table]:
                    var_name = f"{table}_{i}"
                    variable_list.append(f"{table}_{i}")
                    variables[var_name] = solver.BoolVar(var_name)
                    cost_list[f"{table}_{i}"] = self.tuple_price_list[table]

            objective = solver.Objective()
            for var in variable_list:
                objective.SetCoefficient(variables[var], cost_list[var])
            objective.SetMinimization()

            
            for i, item in enumerate(o_results):
                aID_list = item[:table_num]
                for j, table in enumerate(query_tables):
                    var = f"{table}_{aID_list[j]}"
                    solver.Add(variables[var] >= 1)
                    
            # Solve the problem
            status = solver.Solve()
            
            if status == pywraplp.Solver.OPTIMAL:
                # print("Found the solution")
                price = solver.Objective().Value()
                # print(f"Total price: {price}")
                # for var_name in all_variable_list:
                    # print(f"{var_name} = {variables[var_name].solution_value()}")
            else:
                price = -1
                # print("NotFound the solution")  
        else:
            # get the lineage set of each query result
            tuple_lineage_set = defaultdict(list)
            for item in o_results:
                aID_list = item[:table_num]
                tuple_lineage_set[tuple(item[table_num:])].append(aID_list)
                
            
            solver = pywraplp.Solver.CreateSolver('SCIP')  
            if not solver:
                raise Exception("Cannot create the solver")
            
            variable_list = []
            all_variable_list = []
            cost_list = {}
            variable_table = defaultdict(set)
            
            for i, item in enumerate(o_results):
                aID_list = item[:table_num]
                for j, table in enumerate(query_tables):
                    variable_table[table].add(aID_list[j])
            
            # Create variables
            variables = {}
            for table in query_tables:
                for i in variable_table[table]:
                    var_name = f"{table}_{i}"
                    variables[var_name] = solver.BoolVar(var_name)
                    variable_list.append(var_name)
                    all_variable_list.append(var_name)
                    cost_list[var_name] = self.tuple_price_list[table]
            
            i = 0
            for item in tuple_lineage_set.keys():
                for _ in range(len(tuple_lineage_set[item])):
                    var_name = f"result_{i}"
                    variables[var_name] = solver.BoolVar(var_name)
                    all_variable_list.append(var_name)
                    i += 1
            
            # Set the objective function
            objective = solver.Objective()
            for var in variable_list:
                objective.SetCoefficient(variables[var], cost_list[var])
            objective.SetMinimization()
            
            # Add constraints
            i = 0
            for item in tuple_lineage_set.keys():
                last_i = i
                for aID_list in tuple_lineage_set[item]:
                    for j, table in enumerate(query_tables):
                        var = f"{table}_{aID_list[j]}"
                        var2 = f"result_{i}"
                        solver.Add(variables[var] >= variables[var2])
                    i += 1
                solver.Add(solver.Sum([variables[f"result_{k}"] for k in range(last_i, i)]) >= 1)
            
            # Solve the problem
            status = solver.Solve()
            
            if status == pywraplp.Solver.OPTIMAL:
                # print("Found the solution")
                price = solver.Objective().Value()
                # print(f"Total: {price}")
                # for var_name in all_variable_list:
                    # print(f"{var_name} = {variables[var_name].solution_value()}")
            else:
                price = -1
                # print("NotFound the solution")    

        return price





if __name__ == '__main__':
    pricer= VPricer(db, table_size_list, tuple_price_list)
    for mark in mark_sql_list.keys():
        if("A" not in mark):
            sql_list = mark_sql_list[mark]
            pricer.print_required_query(sql_list, mark)
            
