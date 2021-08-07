from datetime import datetime
now = datetime.now()
import pandas as pd
import xml.etree.ElementTree as ET
from progress.bar import Bar
import numpy as np
import requests
import os
import yaml
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient
import re
import colorama
from colorama import Fore
import pyodbc
from fast_to_sql import fast_to_sql as fts
from bs4 import BeautifulSoup
import logging 
import azure.functions as func


def get_records(xml, df, query):
    print(f"{Fore.YELLOW}Getting the data from client...")
    headers = df['headers'][0]
    url = df['url'][0]

    f_index = xml.find("<ns:sqlQuery>")+14
    r_index = xml.find("</ns:sqlQuery>")-1
    

    data = xml.replace(xml[f_index:r_index], query)
    xml_text = requests.post(url, data=data, headers=headers).text
    # now = datetime.now().strftime("%Y_%b_%d_%H_%M_%S_%f")
    # xml_file = open(now+"_unprocessed_response.xml", "w")
    # n = xml_file.write(response)
    # xml_file.close()

    # xml_ = open(now+"_unprocessed_response.xml", 'r')

    # xml_text = response
    # xml_.close()
    # os.remove(now+"_unprocessed_response.xml")
    file = BeautifulSoup(xml_text, 'xml')
    cols = file.findAll("e:string")
    attri = []
    for col in cols:
        attri += [col.text]
    col_name = attri[:int(len(attri)/2)]
    col_type = attri[int(len(attri)/2):]
    records = file.findAll("anyType")
    data = []
    for record in records:
        data += [record.text]

    for i, type_ in enumerate(col_type):
        if type_ == 'string':
            col_type[i] = str
        if type_ == 'dateTime':
            col_type[i] = "datetime64[ns]"
        if type_ == 'integer':
            col_type[i] = int

    num_cols = len(col_name)

    data_ = np.array([data])
    # print(xml_text)
    data_ = np.reshape(data_, (int(len(data)/num_cols), num_cols))

    df = pd.DataFrame(data_, columns=col_name)
    df = df.drop_duplicates()
    # df = df[df['primarykey'] != '']
    print(df.info())
    # df.to_json(now+"_processed_response.json", orient='records')
    print(f"{Fore.GREEN}Records recieved successfully!")
    del [data_, xml_text, file]
    return df

def load_config():
    """
    This function loads the config file.
    """
    # dir_root = os.path.dirname(os.path.abspath(__file__))
    with open("config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def enum_paths(connection_string, container_name):
    file_system = FileSystemClient.from_connection_string(connection_string, file_system_name=container_name)
    paths_ = file_system.get_paths()
    paths = []
    dates = []
    for path in paths_:
        paths.append(path.name)
        match_str = re.search(r'\d{4}-\d{2}-\d{2}', path.name)
        if match_str != None:
            res = datetime.strptime(match_str.group(), '%Y-%m-%d')
            dates.append(res)
    dates = sorted(dates)
    latest_dir = dates[-1].strftime("%Y-%m-%d")+"/"
    latest_path = []
    for path in paths:
        if path.find(latest_dir) != -1:
            latest_path += [path]
    return latest_dir[:-1]

def upload_raw(latest_dir, connection_string, container_name, df):
    print(f"{Fore.YELLOW}Uploading records in datalake, This process might take a while...")
    today = str(datetime.today().strftime("%Y-%m-%d"))
    if latest_dir != today:
        file_system = FileSystemClient.from_connection_string(connection_string, file_system_name=container_name)
        location = "tmp/out/"+today
        file_system.create_directory(location)
    else:
        location = "tmp/out/"+today

    dir_ = location+"/"
    # for df in files:
        # df = pd.read_json(fl)
        # os.remove(fl)
        # fl = fl.replace('unprocessed', 'processed')
        # df.to_json(fl, orient='records')
    now = datetime.now().strftime("%Y_%b_%d_%H_%M_%S_%f")
    file = DataLakeFileClient.from_connection_string(connection_string, 
                                            file_system_name=container_name, file_path=dir_+now+"_processed_response.json")

    # f = open(fl, 'r')
    # data = f.read()
    # f.close()
    file.upload_data(str(df.to_dict('records')).replace("'", '"'), overwrite=True)
        
    print(f"{Fore.GREEN}Data written into the datalake successfully!!")

# def create_table(table, data, conn):
#     """
#     This function creates a table in the database. Call this function only once or it will raise an error.
#     """
#     try:
#         print(f"{Fore.BLUE}The table is being created...")
#         df = data
#         columns = list(df.columns)
#         query = ''
#         for col in columns:
#             query += col+' VARCHAR(100), '
#         cursor = conn.cursor()
#         cursor.execute(f'CREATE TABLE {table} ({query[:-2]})')#(work_id NVARCHAR(50), Project_Category NVARCHAR(50), project NVARCHAR(50), primarykey NVARCHAR(50), Work_Department NVARCHAR(50), last_updated_on NVARCHAR(50))')

#         conn.commit()
#         print(f"{Fore.GREEN}The table was created successfully!!")
#         return True
#     except Exception as e:
#         print("The table with the name \""+table+"\" already exists, \nWriting the records to the existing table")
#         return False
#         # print(e)

# def write_data_in_sql(table_name, data, config, delta):
#     """
#     This function writes the json file into the sql database table given in 'table_name' argument.
#     """
#     # try:
#     server = config['server']
#     database = config['database']
#     username = config['username']
#     password = config['password']   
#     driver= '{ODBC Driver 17 for SQL Server}'

#     conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

#     table_name = table_name.replace("[", "").replace("]", "")
#     print("The data is being written into the {table} table".format(table=table_name))
#     print(f"{Fore.BLUE}This process might take a while...")
#     query = 'SELECT count(primarykey) FROM '+table_name
#     cursor = conn.cursor()
#     tables_query = """SELECT table_schema [schema],  table_name [name]
#                       FROM INFORMATION_SCHEMA.TABLES
#                       GO"""
#     df = data
#     df = df[df['primarykey']!='']
#     tables_df = pd.read_sql_query(tables_query, conn)
#     ls = list(tables_df['schema']+"."+tables_df['name'])
#     if table_name in ls:
#         destination_shape = int(pd.read_sql_query(query, conn)[''][0])
#         if destination_shape == df.shape[0]:
#             results = pd.DataFrame(columns=df.columns)
#         else:
#             cursor.execute(f"DROP TABLE {table_name}")
#             conn.commit()
#             results = pd.DataFrame(columns=df.columns)
#     else:
#         results = pd.DataFrame(columns=df.columns)
    
#     if results.empty:
#         results = df
#         del [df]
#     else:
#         print("Detecting changes...")
#         change = delta
#         # change = df.loc[~df.set_index(list(df.columns)).index.isin(results.set_index(list(results.columns)).index)]
#         # change = change.astype('string')
#         del [df, results]
#         if len(change.index)!=0 and len(change.index)!=1:
#             print("Changes Detected!")
#             if change.shape[0] <= 5000:
#                 cursor.execute('''
#                     DELETE FROM {table_name} 
#                     WHERE primarykey IN {ppl};'''.format(ppl=tuple(change.primarykey), table_name=table_name))
#                 conn.commit()
#             else:
#                 spaces = [int(i) for i in np.linspace(0, change.shape[0], 100)]
#                 for i in range(len(spaces)-1):
#                     cursor.execute('''
#                         DELETE FROM {table_name} 
#                         WHERE primarykey IN {ppl};'''.format(ppl=tuple(change.primarykey[spaces[i]:spaces[i+1]]), table_name=table_name))
#                     conn.commit()


#         elif len(change.index)==1:
#             print("Changes Detected!")
#             cursor.execute('''
#                 DELETE FROM {table_name} 
#                 WHERE primarykey = \'{ppl}\';'''.format(ppl=change.primarykey.values[0], table_name=table_name))
#             conn.commit()

#         results = change
    
#     if results.empty:
#         print(f"{Fore.GREEN}No changes detected...")
#     elif results.shape[0] <= 20000:
#         print("Writing data to the database")
#         create_statement = fts.fast_to_sql(results, table_name, conn, if_exists="append")
#         conn.commit()
        
#         print(f"{Fore.GREEN}The data was written into the database successfully!!")
#     else:
#         print("Writing data to the database")
#         spaces = [int(i) for i in np.linspace(0, results.shape[0], 100)]
#         with Bar('Writing', fill='#', suffix='%(percent).1f%% - %(eta)ds') as bar:
#             for i in range(len(spaces)-1):
#                 fts.fast_to_sql(results[spaces[i]:spaces[i+1]], table_name, conn, if_exists="append")
#                 bar.next()
#             bar.next()
#         conn.commit()

#         print(f"{Fore.GREEN}The data was written into the database successfully!!")
#     conn.close()

def write_all(df, config):
    """
    This function writes the records into the corresponding tables in the database.
    """

    df = df.astype('string')
    cols = [string.lower() for string in df.columns]
    df.columns = cols
    table = df['tablename'][0]
    df = df.drop('tablename', axis=1)
    # create_table(table=table, data=df, conn=conn)
    # write_data_in_sql(table_name=table, data=df, config=config, delta=delta)
    server = config['server']
    database = config['database']
    username = config['username']
    password = config['password']   
    driver= '{ODBC Driver 17 for SQL Server}'

    conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

    print("Writing data to the database")
    cursor = conn.cursor()
    table = table.replace("[", "").replace("]", "")
    tables_query = """SELECT table_schema [schema],  table_name [name]
                      FROM INFORMATION_SCHEMA.TABLES
                      GO"""
    tables_df = pd.read_sql_query(tables_query, conn)
    ls = list(tables_df['schema']+"."+tables_df['name'])
    if table in ls:
        cursor.execute(f"DROP TABLE {table}")
        conn.commit()
    if df.shape[0] > 10000:
        spaces = [int(i) for i in np.linspace(0, df.shape[0], 50)]
        with Bar('Writing', fill='#', suffix='%(percent).1f%% - %(eta)ds') as bar:
            for i in range(len(spaces)-1):
                fts.fast_to_sql(df[spaces[i]:spaces[i+1]], table, conn, if_exists="append")
                bar.next()
            bar.next()
    
    else:
        fts.fast_to_sql(df, table, conn, if_exists='append')
    
    del [df]

    conn.commit()
    conn.close()
    print(f"{Fore.GREEN}The data was written into the database successfully!!")
        # os.remove(path)

def main(mytimer: func.TimerRequest) -> None:
    
    colorama.init(autoreset=True)

    xml = open("body.xml").read()
    df = pd.read_json("HttpClientConfig_1.json")
    
    config = load_config()

    n_queries = len(df["sqlQueries"][0])

    queries = []
    for i in range(n_queries):
        queries += [df["sqlQueries"][0][i]['SqlQuery']]

    latest_dir = enum_paths(config["azure_storage_connectionstring"], config["json_container"])
    for query in queries:
        try:
            data = get_records(xml, df, query)
            upload_raw(latest_dir, config["azure_storage_connectionstring"], config["json_container"], data)
        # n_queries = len(df["sqlQueries"][0])
        # queries = []
        # for i in range(n_queries):
        #     df["sqlQueries"][0][i]['SqlQuery'] = "<![CDATA["+df["sqlQueries"][0][i]['SqlQuery']+"where last_updated_on >= dateadd(DD,-1,getdate())]]>"

        # delta = get_records(xml, df)
        # print(delta)
            write_all(data, config)
            
            del[data]
        except Exception as e:
            print(e)

    print(f"{Fore.GREEN}The process was completed in {datetime.now()-now}")

