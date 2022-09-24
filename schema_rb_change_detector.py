#%%
import os
import sys
import json
import re
import time
import difflib
from datetime import datetime,timedelta
import requests
from dotenv import load_dotenv  # Handling Passwords and Secret Keys using Environment Variables
from pathlib import Path         #You may explicitly provide a path to your .env file.

env_path=Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

from logging import log
from logging import Logger


#%%

# ====================================================
# defining global variables
# ====================================================

webhook_url=os.environ['WEBHOOK_URL']
slack_bot_user_oauth_token= os.environ['SLACK_BOT_USER_OAUTH_TOKEN']
slack_username= os.environ['SLACK_USERNAME']
slack_channelname= os.environ['SLACK_CHANNELNAME']
github_token= os.environ['GITHUBTOKEN']
schema_file_url= os.environ['SCHEMA_FILE_URL']


now=datetime.now()
current_date_and_time=now.strftime("%D %H:%M:%S")
current_date= now.strftime("%y_%m_%d")


yesterday= now-timedelta(days=1)
yesterday=yesterday.strftime("%y_%m_%d")




# define file names to be compared
file_name_1=f'schema_{yesterday}.rb'
file_name_2=f'schema_{current_date}.rb'





# create log files
change_log_filename = f'change_log_{current_date}.txt'
project_name_and_date= ' Database Schema Change: ' + current_date_and_time + '\n\n'
change_log= open(change_log_filename,'w')
change_log.write(project_name_and_date)





#%%

# ====================================================
# defining functions
# ====================================================


#log process

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


# =========


def github_download(token,url):

    """
    Call     : github_download(token,url)
    Returns   : file_content
    """

    file_url= url + token

    try:
        # log.info("started download")
        response= requests.get(file_url)

        if response==200:
            file_content=response.text
            #log.info("Downloaded file")
            return file_content

    except:

        status=requests.get(file_url).status_code
        print('Error: ',status)
        return False




#%%
# =========

def write_file(data,file_name):

    """
    Call     :  write_file(data,file_name)
    Returns   : True if able to write the data as a file,otherwise False
    """

    try:
        file= open(file_name,'w')
        file.write(data)
        return True

    except:
        print(f'Error: Could not write {file_name}')
        return False


#%%
# =========

#parse table names from the schema file

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

#%%
#======================

def get_file_content(file_name):
    content= ''

    with open(file_name,'r') as f:
        content=f.read()
    return content




def post_file_to_slack( text, file_name, channelname, file_bytes, file_type= None, title=None):
    return requests.post(
        'https://slack.com/api/files.upload',
        {
            'token': slack_bot_user_oauth_token,
            'filename': file_name,
            'channels': channelname,
            'filetype': 'text',
            'initial_comment': None,
            'title': None

        },
    files= {'file':file_bytes}).json()
    

# %%
# ==================================
# 1. Download new schema file
# ==================================

try:
    data= github_download(github_token, schema_file_url)
     # write_file(data, file_name_2)

except:

    log.info('Error: Could not download schema.rb')



# %%
# ==================================
# 2. Read files
# ==================================

with open(file_name_1,'r') as file_1:  
  file_1_text=file_1.readlines()



with open(file_name_2,'r') as file_2:  
  file_2_text=file_2.readlines()


# %%
# ==================================
# 3.  Test 1:  Compare if there are any changes
# ==================================

try:
    assert(file_1_text==file_2_text)
    change_log.write('Test 0: PASS  -  No Schema Change Detected\n')
    log.info('Test 0: PASS  -  No Schema Change Detected\n')

except:
    change_log.write('Test 0: FAIL  - Schema Change Detected\n')
    log.info('Test 0: FAIL  - Schema Change Detected\n')

# ==================================
# 3.  Test 2:  Compare if tables changed
# ==================================

#table_names in table_1 or old file
previous_schema= parse_tables(file_1_text)
new_schema= parse_tables(file_2_text)

previous_schema_tables= set(previous_schema.keys())


#table_names in table_1 or old file
new_schema_tables= set(new_schema.keys())

    
    # compare files now;

try:
    assert(previous_schema_tables==new_schema_tables)
    change_log.write('Test 1: PASS  -  No changes made to the table names\n')
    log.info('Test 1: PASS  -  No changes made to the table names\n')

except:
    change_log.write('Test 1: FAIL  - Table Name Change Detected\n')
    log.info('Test 0: FAIL  - Table Name Change Detected\n')

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
# 3.  Test 3:  Compare column definitions
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
            #change_log.write('Test 2: PASS  - No column changes were detected ')
            continue

    except:
        if table_name not in new_schema_tables:
            continue
        else:
            rows_1=set(previous_schema[table_name])
            rows_2=set(new_schema[table_name])

            #isolate the error
            if table_counter==0:
                #change_log.write('\n Test 2: FAIL - Column changes detected \n')
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
                {str(rows_2.difference(rows_1))} 
                """ 
                )
change_log.close()


#%%
# ==================================
# 4.  Post Log to Slack
# ==================================

post_file_to_slack(
    text=None,
    file_name=change_log_filename,
    channelname=slack_channelname,
    file_bytes=get_file_content(change_log_filename)
)