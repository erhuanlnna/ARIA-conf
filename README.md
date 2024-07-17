We describe how to employ ARIA to price the select-project-join-aggregate (SPJA) queries on the MySQL database server.
This README provides instructions on how to set up and run the default experiments, including data preparation, environment setup, and execution of experiments. Additionally, we guide you through various experiment variations, such as adjusting the support size, scale factor, and pricing queries with different selectivities, limit numbers, distinct, and aggregate queries.

# Environment setup  \&  Data preparation

## Environment setup

Before running any experiments, ensure your environment is correctly set up. This project requires:
- Python: 3.7
- MySQL Server (8.0 or newer)
- Necessary libraries:
  - Mysql
  - Mysql-connector-python
  - Mysqlclient
  - Pymysql
  - Pandas
  - Sqlglot
  - sqlalchemy
  - ortools 
All libraries are provided in the `requirements.txt`.
Make sure Python, MySQL, and other libraries are properly installed and configured on your system. It's also recommended to create a virtual environment for Python dependencies to avoid any conflicts with other projects.

## Data preparation

There are four datasets used in the experiments:
- [Walmart](https://aws.amazon.com/marketplace/pp/prodview-zaejml2253r7k)
- [Employment](https://aws.amazon.com/marketplace/pp/prodview-yp5x2esst5dji#offers)
- [TPC-H](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
- [SSB](https://github.com/eyalroz/ssb-dbgen)

The `sql` files of the first two datasets are provided, which can be used to directly import the database to mysql via the following codes.
```
mysql -u username -p walmart < walmart.sql
mysql -u username -p employment < employment.sql
```
For the other datasets (TPC-H, and SSB), follow the instructions on their respective websites to generate the datasets and import them (i.e., `tpch1g`, `tpch2g`,..., `tpch5g`, `ssb1g`,..., `ssb5g`) into MySQL.


# Reproduce the experiments 


## Run from scratch (Data have been prepared well)
1. Replace the `user` the `password` with your username and password of MySQL in the `dbSettings.py`
2. Generate the support sets for `ARIA` and `QIRANA` by runing the codes.
```bash
python ar_generate_db.py
python qa_generate_db.py
```
The generated support sets are automatically imported into the database.
4. Run the following codes and the corresponding results are in the `rs` folder.
```bash
python test_ARIA_online.py
python test_VBP_online.py
python test_PBP_online.py
python test_QIRANA_online.py
python exp_compare_online.py # table 6
python column-correlration.py # table 7
python free-tuples.py #table 8
python exp_dependency_online.py # figure 10 (only support Walmart dataset)
python exp_distinct_agg_online.py # figures 11 and 12 (only support Walmart and Employment datasets)
```
3. To reproduce the experiments of varying the support size in Figure 6, change the size from 0 to 1 in the `test_ARIA_online.py`, i.e.,
```python
size = 0.2 * 0 # 0.2 * 1, 0.2 * 2, 0.2 * 3, 0.2 *4, 0.2 *5
```
Run the `test_ARIA_online.py` with different sizes and run `exp_support_size_online.py` to obtain the results.

For other dataset (i.e., `Employment`, `TPCH` and `SSB`) replace the content of  `test_queries.py` with that of `test_queries_employment.py` and repeat the above operations.
In addition, one have to run the codes to produce the results of Figures 8 and 9 by running
```bash
python exp_selectivity.py # figure 8 (only support TPC-H and SSB datasets)
python exp_join.py  # figure 9 (only support TPC-H and SSB datasets)
```


## Directly run the experiments without preparing data
We have provided the necessary query results in the `pre_rs` folder on `Walmart` database, so that you can directly run the code to reproduce the experimental results.
Run the following codes and the corresponding results are in the `rs` folder.
```bash
python test_ARIA_offline.py
python test_VBP_offline.py
python test_PBP_offline.py
python test_QIRANA_offline.py
```
In this way, the execution time of price computation for each method is obtained.
Moreover, you can the following codes to obtain the final results in the paper.
```bash
python exp_compare_offline.py # table 6
python column-correlration.py # table 7
python exp_dependency_offline.py # figure 10
python exp_distinct_agg_offline.py # figures 11 and 12
```
