
import requests
import os
import snowflake.connector as sf
import toml


print('Loading function')

#---------------------------------
#5- snowflake_query: to execute Snowflake sql statements.
def snowflake_query(cursor, query):
    cursor.execute(query)

#---------------------------------
#4- snowflake_func: contains the connection to snowflake + list for all sql queries. 
def snowflake_func (user,password,account, role ,warehouse, database, schema ,table , stage_name, file_format_name ,file_path, file_name):
    
    #Note : take the space seriously in the Query SQL or it will make alot of mistake
    #A- connect to snowflake 
    conn = sf.connect(user = user , password = password, \
    account = account, warehouse = warehouse, \
    database = database, schema = schema, role = role)
    
    cursor = conn.cursor() #now we are on snowflake
    
    #B- create queries list contains :
    queries = [
        f"use warehouse {warehouse};", 
        f"use schema {schema};", 
        f"create or replace file format {file_format_name} type='csv' field_delimiter=',' ;", 
        f"create or replace stage {stage_name} file_format={file_format_name};", 
        f"put file://{file_path} @{stage_name};", #RUN PUT STATEMENT
        f"list @{stage_name} ;", 
        f"truncate table {schema}.{table}", 
        f"copy into {schema}.{table} from @{stage_name}/{file_name} file_format={file_format_name} on_error='continue';" #COPY INTO TABLE
    ]
    
    #C- create loop through the queries and execute them using snowflake_query function
    for query in queries:
        snowflake_query(cursor, query)

#--------------------------------
# The main function:
def lambda_handler(event, context):
    
    app_config = toml.load('config.toml')
    #0- PARAMETER:
    
    #a) request & os parameters
    url = app_config['url']['url']
    destination_folder = app_config['destination']['destination_folder']
    file_name = app_config['destination']['file_name']
    
    #b) snowflake parameters
    user =  os.environ['user']
    password = os.environ['password']
    account = os.environ['account']
    role = os.environ['role']
    warehouse = app_config['snowflake']['warehouse']
    database = app_config['snowflake']['database']
    schema =  app_config['snowflake']['schema']
    file_format_name = app_config['snowflake']['file_format_name']
    stage_name = app_config['snowflake']['stage_name']
    table = app_config['snowflake']['table']
  
    #1- grab inventory from url
    response  = requests.get(url)
    response.raise_for_status() #?????
    
    #2-savee the grab data into /tmp folder + print it
    file_path = os.path.join(destination_folder, file_name)
    #open the file & write 
    with open(file_path, 'wb') as file: #write binar = I want it exact the same witout any changes in csv file
        file.write(response.content)
        
    #open the file & read 
    with open(file_path, 'r') as file:
        #take file content to read
        file_content = file.read()
        print(file_content)
        
    #3-call the snowflake_func function    
    connect_SF = snowflake_func(user,password,account,role, warehouse, database ,schema ,table , stage_name, file_format_name ,file_path, file_name)

    
       
    print("file uploaded successfully")

    return {
            'statusCode': 200 ,
            'body' : 'File uploaded successfully into snowflake.'
        }
   
    
    
    
    