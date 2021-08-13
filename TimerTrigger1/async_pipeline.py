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
from tqdm import tqdm
import math
import asyncio 
import aiohttp
import time


async def get_tasks(session, xml, df, query, trip, primary_key, batch_size):
    headers = df['headers'][0]
    url = df['url'][0]

    f_index = xml.find("<ns:sqlQuery>")+14
    r_index = xml.find("</ns:sqlQuery>")-1
    
        
    # for trip in range(n_trips):
    batch_query = f"""<![CDATA[{query} 
ORDER BY {primary_key} 
OFFSET ({trip})*{str(batch_size)} ROWS 
FETCH NEXT {str(batch_size)} ROWS ONLY]]>"""
    data = xml.replace(xml[f_index:r_index], batch_query)
    # tasks.append(asyncio.create_task(session.post(url, data=data, headers=headers)))
    async with session.post(url, data=data, headers=headers) as response:
        task = await response.text()
        return task

async def get_symbols(xml, df, query, s_trips, l_trips, primary_key, batch_size):
    conn = aiohttp.TCPConnector(limit=None)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = []
        for trip in range(s_trips, l_trips):
            tasks.append(asyncio.ensure_future(get_tasks(session, xml, df, query, trip, primary_key, batch_size)))
        
        responses = await asyncio.gather(*tasks)
        master_df = pd.DataFrame()
        # xml_text = ''
        # for i, response in enumerate(responses):
        #     if i == 0:
        #         xml_text+=response
        #         responses[i] = ''
        #     else:
        #         xml_text = xml_text[:xml_text.index('</d:QueryResults>')]+response[response.index('<e:ArrayOfanyType>'):response.index('</d:QueryResults>')]+'<'+xml_text[xml_text.index('</d:QueryResults>')+1:]
        #         responses[i] = ''
        for i, response in enumerate(responses):
        #     #   print(await response.read())
            xml_text = response
            file = BeautifulSoup(xml_text, 'xml')
            xml_text = ''
            cols = file.findAll("e:string")
            attri = []
            for col in cols:
                attri += [col.text]
            col_name = attri[:int(len(attri)/2)]
            # col_type = attri[int(len(attri)/2):]
            records = file.findAll("anyType")
            file = ''
            data = []
            for record in records:
                data += [record.text]
            records = ''
            # for i, type_ in enumerate(col_type):
            #     if type_ == 'string':
            #         col_type[i] = str
            #     if type_ == 'dateTime':
            #         col_type[i] = "datetime64[ns]"
            #     if type_ == 'integer':
            #         col_type[i] = int

            num_cols = len(col_name)
            if num_cols!=0:
                data_ = np.array([data])
                
                # # print(xml_text)
                data_ = np.reshape(data_, (int(len(data)/num_cols), num_cols))
                data = ''
                df = pd.DataFrame(data_, columns=col_name)
                data_ = ''
                df = df.drop_duplicates()
                df = df.astype('string')
                cols = [string.lower() for string in df.columns]
                df.columns = cols
                df = df.drop('tablename', axis=1)
                master_df = master_df.append(df, ignore_index=True)
            responses[i] = ''
        responses = []
    return master_df

def get_records(xml, df, query):
    
    headers = df['headers'][0]
    url = df['url'][0]

    f_index = xml.find("<ns:sqlQuery>")+14
    r_index = xml.find("</ns:sqlQuery>")-1
    

    data = xml.replace(xml[f_index:r_index], query)
    xml_text = requests.post(url, data=data, headers=headers).text
    # print(xml_text)
    # now = datetime.now().strftime("%Y_%b_%d_%H_%M_%S_%f")
    # xml_file = open(now+"_unprocessed_response.xml", "w")
    # n = xml_file.write(response)
    # xml_file.close()

    # xml_ = open(now+"_unprocessed_response.xml", 'r')

    # xml_text = response
    # xml_.close()
    # os.remove(now+"_unprocessed_response.xml")
    
    file = BeautifulSoup(xml_text, 'xml')
    # print(file)
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

    # for i, type_ in enumerate(col_type):
    #     if type_ == 'string':
    #         col_type[i] = str
    #     if type_ == 'dateTime':
    #         col_type[i] = "datetime64[ns]"
    #     if type_ == 'integer':
    #         col_type[i] = int

    num_cols = len(col_name)
    if num_cols!=0:
        data_ = np.array([data])
        # # print(xml_text)
        data_ = np.reshape(data_, (int(len(data)/num_cols), num_cols))

        df = pd.DataFrame(data_, columns=col_name)
        df = df.drop_duplicates()
        df = df.astype('string')
        cols = [string.lower() for string in df.columns]
        df.columns = cols
        # df = df[df['primarykey'] != '']
        # print(df.info())
        # df.to_json(now+"_processed_response.json", orient='records')
        
        del [xml_text, file]
    else:
        print(xml_text)
    return df

def load_config():
    """
    This function loads the config file.
    """
    # dir_root = os.path.dirname(os.path.abspath(__file__))
    with open("TimerTrigger1/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)


def main():#mytimer: func.TimerRequest) -> None:
    
    colorama.init(autoreset=True)

    xml = open("TimerTrigger1/body.xml").read()
    df = pd.read_json("TimerTrigger1/HttpClientConfig.json")
    
    config = load_config()

    n_queries = len(df["sqlQueries"][0])

    queries = []
    for i in range(n_queries):
        queries += [df["sqlQueries"][0][i]['SqlQuery']]

    # latest_dir = enum_paths(config["azure_storage_connectionstring"], config["json_container"])
    for query in queries:
        # try:
        print(f"{Fore.YELLOW}Getting the data from client...")
        # token_query = "<![CDATA["+query[:6]+" top 1 "+query[7:]+"]]>"
        token_query = list(map(str.split, [query]))[0]
        token_query = [token.replace(',', '').replace("'", '').lower() for token in token_query]
        dest_table = token_query[token_query.index('tablename')-2].replace("[", "").replace("]", "")
        source_table = token_query[token_query.index('from')+1].replace("[", "").replace("]", "")
        primary_key = token_query[token_query.index('primarykey')-2]
        # table_def = get_records(xml, df, table_query)
        print("Destination table: "+dest_table)
        print("Source table: "+source_table)
        print("Primary key: "+primary_key)
        # cols = [string.lower() for string in table_def.columns]
        # table_def.columns = cols
        # table = table_def['tablename'][0]
        # primary_key = table_def['primarykey'][0]
        # table = table.replace("[", "").replace("]", "")
        # print(table)
        # # schema_name = table[:table.find('.')]
        # # table_name = table[table.find('.')+1:]
        row_query = f"SELECT COUNT(*) FROM {source_table}"
        n_rows = int(get_records(xml, df, row_query).iloc[0])
        print(n_rows)
        batch_size = 2500
        n_trips = math.ceil(n_rows/batch_size)
        # # spaces = np.array([int(i) for i in np.linspace(0, rows, 100)])
        
        # # create_table(table=table, data=df, conn=conn)
        # # write_data_in_sql(table_name=table, data=df, config=config, delta=delta)
        server = config['server']
        database = config['database']
        username = config['username']
        password = config['password']   
        driver= '{ODBC Driver 17 for SQL Server}'
        spaces = [int(i) for i in np.linspace(0, n_trips, 10)]
        data = pd.DataFrame()
        if n_rows>100000:
            for i in tqdm(range(len(spaces)-1)):
                df2 = asyncio.run(get_symbols(xml, df, query, s_trips=spaces[i], l_trips=spaces[i+1], primary_key=primary_key, batch_size=batch_size))
                data = data.append(df2, ignore_index=True)
                print(data.info())
        else:
            query = f"<![CDATA[{query}]]>"
            data = get_records(xml, df, query)
            data = data.drop('tablename', axis=1)
            print(data.info())
        # print(data.info())
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        
        
        print("Writing data to the database")
        
        if data.shape[0]>10000:
            spaces = [int(i) for i in np.linspace(0, data.shape[0], 50)]
            
            for i in tqdm(range(len(spaces)-1)):
                if i==0:
                    create_statement = fts.fast_to_sql(data[spaces[i]:spaces[i+1]], dest_table, conn, if_exists="replace")
                else:
                    create_statement = fts.fast_to_sql(data[spaces[i]:spaces[i+1]], dest_table, conn, if_exists="append")
                
        else:
            create_statement = fts.fast_to_sql(data, dest_table, conn, if_exists="replace")

        conn.commit()
        conn.close()
        
        
        print(f"{Fore.GREEN}Records recieved successfully!")
        
        # del [data]
        # except Exception as e:
        #     print(e)

    print(f"{Fore.GREEN}The process was completed in {datetime.now()-now}")

main()