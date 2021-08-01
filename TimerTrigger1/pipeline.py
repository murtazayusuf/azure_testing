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


def get_records(xml, df):
    print(f"{Fore.YELLOW}Getting the data from client...")
    headers = df['headers'][0]
    url = df['url'][0]

    f_index = xml.find("<ns:sqlQuery>")+14
    r_index = xml.find("</ns:sqlQuery>")-1

    n_queries = len(df["sqlQueries"][0])
    queries = []
    for i in range(n_queries):
        queries += [df["sqlQueries"][0][i]['SqlQuery']]

    loc_files = []
    for query in queries:
        data = xml.replace(xml[f_index:r_index], query)
        xml_text = requests.post(url, data=data, headers=headers).text
        now = datetime.now().strftime("%Y_%b_%d_%H_%M_%S_%f")
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

        data_ = np.reshape(data_, (int(len(data)/num_cols), num_cols))

        df = pd.DataFrame(data_, columns=col_name)
        df = df.drop_duplicates()
        # df = df[df['primarykey'] != '']
        print(df.info())
        df.to_json(now+"_processed_response.json", orient='records')
        loc_files += [now+"_processed_response.json"]
    print(f"{Fore.GREEN}Records recieved successfully!")
    return loc_files

def load_config():
    """
    This function loads the config file.
    """
    # dir_root = os.path.dirname(os.path.abspath(__file__))
    with open("TimerTrigger1/config.yaml", "r") as yamlfile:
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

def upload_raw(latest_dir, connection_string, container_name, files):
    print(f"{Fore.YELLOW}Uploading records in datalake, This process might take a while...")
    today = str(datetime.today().strftime("%Y-%m-%d"))
    if latest_dir != today:
        file_system = FileSystemClient.from_connection_string(connection_string, file_system_name=container_name)
        location = "tmp/out/"+today
        file_system.create_directory(location)
    else:
        location = "tmp/out/"+today

    dir_ = location+"/"
    processed = []
    for fl in files:
        df = pd.read_json(fl)
        # os.remove(fl)
        # fl = fl.replace('unprocessed', 'processed')
        df.to_json(fl, orient='records')
        file = DataLakeFileClient.from_connection_string(connection_string, 
                                                file_system_name=container_name, file_path=dir_+fl)

        f = open(fl, 'r')
        data = f.read()
        f.close()
        file.upload_data(data, overwrite=True)
        processed += [fl]
        
    print(f"{Fore.GREEN}Data written into the datalake successfully!!")
    return processed

def create_table(table, data, conn):
    """
    This function creates a table in the database. Call this function only once or it will raise an error.
    """
    try:
        print(f"{Fore.BLUE}The table is being created...")
        df = data
        columns = list(df.columns)
        query = ''
        for col in columns:
            query += col+' VARCHAR(100), '
        cursor = conn.cursor()
        cursor.execute(f'CREATE TABLE {table} ({query[:-2]})')#(work_id NVARCHAR(50), Project_Category NVARCHAR(50), project NVARCHAR(50), primarykey NVARCHAR(50), Work_Department NVARCHAR(50), last_updated_on NVARCHAR(50))')

        conn.commit()
        print(f"{Fore.GREEN}The table was created successfully!!")
        return True
    except Exception as e:
        print("The table with the name \""+table+"\" already exists, \nWriting the records to the existing table")
        return False
        # print(e)

def write_data_in_sql(table_name, data, conn):
    """
    This function writes the json file into the sql database table given in 'table_name' argument.
    """
    try:
        print("The data is being written into the {table} table".format(table=table_name))
        print(f"{Fore.BLUE}This process might take a while...")
        query = 'SELECT * FROM '+table_name
        cursor = conn.cursor()
        results = pd.read_sql_query(query, conn)
        df = data
        df = df[df['primarykey']!='']
        if results.empty:
            results = df
        else:
            print("Detecting changes...")
            change = df.loc[~df.set_index(list(df.columns)).index.isin(results.set_index(list(results.columns)).index)]
            change = change.astype('string')
            if len(change.index)!=0 and len(change.index)!=1:
                cursor.execute('''
                    DELETE FROM {table_name} 
                    WHERE primarykey IN {ppl};'''.format(ppl=tuple(change.primarykey), table_name=table_name))
                conn.commit()
            elif len(change.index)==1:
                cursor.execute('''
                    DELETE FROM {table_name} 
                    WHERE primarykey = \'{ppl}\';'''.format(ppl=change.primarykey[0], table_name=table_name))
                conn.commit()

            results = change
            
        if results.empty:
            print(f"{Fore.GREEN}No changes detected.")
        elif results.shape[0] <= 20000:
            create_statement = fts.fast_to_sql(results, table_name, conn, if_exists="append")
            conn.commit()
            
            print(f"{Fore.GREEN}The data was written into the database successfully!!")
        else:
            spaces = [int(i) for i in np.linspace(0, results.shape[0], 100)]
            with Bar('Writing', fill='#', suffix='%(percent).1f%% - %(eta)ds') as bar:
                for i in range(len(spaces)-1):
                    fts.fast_to_sql(results[spaces[i]:spaces[i+1]], table_name, conn, if_exists="append")
                    bar.next()
                bar.next()
            conn.commit()
            
            # print(tuple(change.primarykey))
    except Exception as e:
        print(f"{Fore.RED}Following error occurred during execution:")
        print(str(e))

def write_all(files, conn):
    """
    This function writes the records into the corresponding tables in the database.
    """
    
    for path in files:
        df = pd.read_json(path)
        df = df.astype('string')
        cols = [string.lower() for string in df.columns]
        df.columns = cols
        table = df['tablename'][0]
        df = df.drop('tablename', axis=1)
        create_table(table=table, data=df, conn=conn)
        write_data_in_sql(table_name=table, data=df, conn=conn)
        os.remove(path)

def main(mytimer: func.TimerRequest) -> None:
    
    colorama.init(autoreset=True)

    xml = open("TimerTrigger1/body.xml").read()
    df = pd.read_json("TimerTrigger1/HttpClientConfig.json")
    
    config = load_config()
    server = config['server']
    database = config['database']
    username = config['username']
    password = config['password']   
    driver= '{ODBC Driver 17 for SQL Server}'
    conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

    files = get_records(xml, df)
    latest_dir = enum_paths(config["azure_storage_connectionstring"], config["json_container"])

    files = upload_raw(latest_dir, config["azure_storage_connectionstring"], config["json_container"], files)

    write_all(files, conn)

    conn.close()
    print(f"{Fore.GREEN}The process was completed in {datetime.now()-now}")
