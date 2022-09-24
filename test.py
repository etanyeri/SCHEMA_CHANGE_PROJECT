
#%%
import os
import sys
import json
import re
import time
import difflib
from datetime import datetime,timedelta
import requests
from logging import log
from logging import Logger
from dotenv import load_dotenv     #Handling Passwords and Secret Keys using Environment Variables
from pathlib import Path           #You may explicitly provide a path to your .env file


env_path=Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

githubtoken= os.environ['GITHUBTOKEN']


#============================
#  DEFINE  TIMESTAMPS
#============================
now=datetime.now()
current_date_and_time=now.strftime("%D %H:%M:%S")
current_date= now.strftime("%y_%m_%d")

yesterday= now-timedelta(days=1)
yesterday=yesterday.strftime("%y_%m_%d")



#============================
# DEFINE  FILE NAMES TO BE COMPARED
#============================
file_name_1=f'schema_{yesterday}.rb'
file_name_2=f'schema_{current_date}.rb'



#============================
#  CREATE LOG FILES
#============================
change_log_filename = f'change_log_{current_date}.txt'
project_name_and_date= ' Database Schema Change: ' + current_date_and_time + '\n\n'
change_log= open(change_log_filename,'w')
change_log.write(project_name_and_date)



#==============================
#  CREATE LOG PROCESS FUNCTION
#==============================
def _get_logger(name) -> Logger:
    from logging import Formatter,Logger,basicConfig,getLogger,StreamHandler,DEBUG,INFO

    date_format = '%m,%d,%Y %I:%M:%S %p'
    log_format= '%(asctime)s %(levelname)-8s %(name)-15s %(message)s'
    basicConfig(filename= 'etl.log',format=log_format,datefmt=date_format)

    log: Logger = getLogger('Logging')
    log.setLevel(INFO)
    ch=StreamHandler()
    ch.setLevel(DEBUG)
    formatter= Formatter(log_format)
    ch.setFormatter(formatter)
    log.addHandler(ch)

    return log

log: Logger = _get_logger('Logging:')




#================================
#  CREATE PARSE RB FILE FUNCTION
#================================
def parse_tables(schema_rb):
    """
    Call     :  parse_tables(schema_rb)
    Returns   : A dictionary of table names as key and column definitions from the DDL
    """

    tables={}
    for i in schema_rb:

        try:
            if "create_table" in i:
                table_name= re.search('".*"',i).group().replace('"','') #parse table name

                #each table name will be a key in the table_names dictionary
                tables.update({table_name:[]}) 
                     
            #following rows will be its value
            elif "end" not in i:
                tables[table_name].append(i.replace('\n',''))

        except:
            continue
    return tables




#=============================================================================
#  RECEIVE DAILY schema.rb file FROM REPO LINK AND WRITE THE CONTENT TO THE DEFINED FILES
#=============================================================================
url= os.environ['SCHEMA_FILE_URL']
#url="https://gist.githubusercontent.com/nickychow/2817242/raw/14bb70de2c61ff36ba9862d15bcec581759805e1/schema.rb"
# schema_daily=requests.get(url).text


# # write the today's schema info into the file_2;
# try:
#     with open(file_name_2,'w') as file_2:          
#         file_2.write(schema_daily)           
        
#         #OR  
#     # file=open(f"schema_{current_date}.rb",'w')   
#     # file.write(schema_daily)               

# except:
#     print(f"ERROR: Could not write {file_2}")



# # write the yesterday's schema info into file_1 if not in the directory;
# if not os.path.isfile(file_name_1):

#     with open(file_name_1,'w') as file_1:         
#      file_1.write(schema_daily) 

def github_download(url):
    """
    Call     : github_download(url)
    Returns   : file_content
    """
    
    try:
        file_content = requests.get(url).text
        status=requests.get(url).status_code

        if status==200:
            log.info("Downloaded file")
            return file_content

    except:
        print('Error: ',status)
        return False

github_download(url)     
#%%

def write_file(data,file_name):

    try:
        file= open(file_name,'w') 
        file.write(data)
        return True

    except:
        print(f'Error: Could not write {file_name}')
        return False



def get_file_content(file_name):
    content= ''

    with open(file_name,'r') as f:
        content=f.read()
    return content

# ==================================
#  Download new schema file
# ==================================

try:
    data= github_download(url)
    write_file(data,file_name_2)

except:
    log.info('Error: Could not download schema.rb')


if not os.path.isfile(file_name_1):
   with open(file_name_1,'w') as file_1:         
    write_file(data,file_name_1) 



#=================================
# READ FILES
# ===============================
with open(file_name_1,'r') as file_1:  
  file_1_text=file_1.readlines()



with open(file_name_2,'r') as file_2:  
  file_2_text=file_2.readlines()




# ===============================================================
#  Compare if any changes found between files AS SCHEMA LEVEL
# ===============================================================
try:
    assert(file_1_text==file_2_text)
    change_log.write('Test 0: PASS  -  No Schema Change Detected\n')
    log.info('Test 0: PASS  -  No Schema Change Detected\n')

except:
    change_log.write('Test 0: FAIL  - Schema Change Detected\n')
    log.info('Test 0: FAIL  - Schema Change Detected\n')





# ==================================
# COMPARE IF TABLE/S CHNAGED ON SCHEMA
# ==================================
previous_schema= parse_tables(file_1_text)
new_schema= parse_tables(file_2_text)


#table_names in yesterday's file
previous_schema_tables= set(previous_schema.keys())


#table_names in today's file
new_schema_tables= set(new_schema.keys())


try:
    assert(previous_schema_tables==new_schema_tables)
    change_log.write('Test 1: PASS  -  No changes made to the table names\n\n')
    log.info('Test 1: PASS  -  No changes made to the table names\n')

except:
    change_log.write('Test 1: FAIL  - Table Name Change Detected\n')
    log.info('Test 1: FAIL  - Table Name Change Detected\n')

    if len(previous_schema_tables.difference(new_schema_tables))>0:
        change_log.write(
            f"""
        Deleted Tables:
                {previous_schema_tables.difference(new_schema_tables)}\n
            """
        )

    if len(new_schema_tables.difference(previous_schema_tables))>0:
        change_log.write(
            f"""
        New Tables:
                {new_schema_tables.difference(previous_schema_tables)}\n
            """
        )



# ==================================
# COMPARE COLUMN CHANGES
# ==================================

table_counter=0
for table_name in previous_schema_tables:

    try:
        if table_name not in new_schema_tables:
            continue

        else:
            rows_1=set(previous_schema[table_name])
            rows_2=set(new_schema[table_name])
            assert(rows_1==rows_2)
            #change_log.write('Test 2: PASS  -  No column changes were detected \n')
            continue
            
    except:
        if table_name not in new_schema_tables:
            continue
        
        else:
            rows_1=set(previous_schema[table_name])
            rows_2=set(new_schema[table_name])

            #isolate the error
            if 1==1:
                #change_log.write('Test 2: FAIL  -  Column changes detected \n')
                table_counter+=1
                change_log.write(f"""
    Table Name: {table_name} """ )
               

            if len(rows_1.difference(rows_2))>0:
                #Possible Deletions
                change_log.write(f"""   
        Column Differences - Previous Schema: 
                {rows_1.difference(rows_2)} 
                """ 
                )   
   
            if len(rows_2.difference(rows_1))>0:
                #Possible Additiona
                change_log.write(f"""   
        Column Differences - New Schema: 
                {rows_2.difference(rows_1)} 
                """ 
                )
change_log.close()



#%%





