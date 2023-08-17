from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from pyspark.sql.functions import *
from ETL import Extract,Transform,Load
from datetime import datetime
import os
import pandas as pd


  
def extract_price_stock_1(ti):
    code = ti.xcom_pull(task_ids = ["Crawl_StockCode"])[0]
    length_code = len(code)
    data = dag_extract.crawl_data(code,date_to,date_from,0,(length_code)//3)
    if data is None:
        return None
    data = pd.DataFrame(data)
    data.to_csv('/home/leader/Documents/airflow-environment/data_stock/stock_price_1.csv',index = False)
    return True

def extract_price_stock_2(ti):
    code = ti.xcom_pull(task_ids = ["Crawl_StockCode"])[0]
    length_code = len(code)
    data = dag_extract.crawl_data(code,date_to,date_from,(length_code)//3,(length_code)//3 * 2)
    if data is None:
        return None
    data = pd.DataFrame(data)
    data.to_csv('/home/leader/Documents/airflow-environment/data_stock/stock_price_2.csv',index = False)
    return True

def extract_price_stock_3(ti):
    code = ti.xcom_pull(task_ids = ["Crawl_StockCode"])[0]
    length_code = len(code)
    data = dag_extract.crawl_data(code,date_to,date_from,(length_code)//3 * 2,(length_code))
    if data is None:
        return None
    data = pd.DataFrame(data)
    data.to_csv('/home/leader/Documents/airflow-environment/data_stock/stock_price_3.csv',index = False)
    return True

def merge_data(ti):
    status = ti.xcom_pull(task_ids = ['Crawl_Stock_Price_1',
                                      'Crawl_Stock_Price_2',
                                      'Crawl_Stock_Price_3'])
    if status == [True,True,True]:
        path = '/home/leader/Documents/airflow-environment/data_stock/'
        frames = []
        for file in os.listdir(path):
            if file.endswith('.csv') and file != 'full_data.csv':
                filepath = path + file
                df = pd.read_csv(filepath)
                frames.append(df) 
                result = pd.concat(frames)
        path = path + 'full_data.csv'
        result.to_csv(path,index = False)
        return path
    return 'None'
    

def transform_data_spark(ti):
    
    path = ti.xcom_pull(task_ids = ['Merge_Data'])[0] # return path or None value
    if path == 'None': 
        return '/home/leader/Documents/airflow-environment/data_dim/full_data_blank.csv'
    dag_transform = Transform()
    df, path = dag_transform.transform(path)
    df.to_csv(path,index =False)
    return path 
    


# def load_bigQuery(ti):
#      status = ti.xcom_pull(task_ids = ['Transform_data'])
#      if status is None:
#           print("Don't have data to upload bigQuery !") 
#           return None
#      dags_load.load_data_into_Bigquery()
     
def load_MySQL(ti):
    dags_load = Load()
    path = ti.xcom_pull(task_ids = ['Transform_data'])[0]
    dags_load.load_data_into_MySQL(path,'full_data')
    return True
path = '/home/leader/Documents/airflow-environment/sql/'    
sql_file = {
    'create_table': open(path + 'create_table_query.sql',mode ='r').read(),
    'datetime_dim': open(path + 'datetime_dim.sql',mode = 'r').read(),
    'price_dim': open(path + 'price_dim.sql',mode = 'r').read(),
    'OHLC_dim': open(path + 'OHLC_dim.sql',mode = 'r').read(),
    'ad_dim': open(path + 'ad_dim.sql',mode = 'r').read(),
    'vol_val_dim': open(path + 'vol_val_dim.sql',mode = 'r').read(),
    'change_dim': open(path + 'change_dim.sql',mode = 'r').read(),
    'fact_table': open(path + 'fact_table.sql',mode = 'r').read(),
}
default_args = {
     'owner' : 'HoangTam',
     'retries' : 2,
     'retry_delay' : timedelta(minutes=2)
}            
    
with DAG(dag_id='Stock_Pipelines',
    start_date=datetime(2023,8,16),
    schedule_interval="30 15 * * *",
    default_args =default_args,
    catchup = False ) as dag:
    
    #    Dags Extract
    dag_extract = Extract() # Functions of dags.ETL
    to_date = from_date = datetime.strftime(datetime.now(),'%Y-%M-%d')


    Extract_Stockcode = PythonOperator(task_id = 'Crawl_StockCode',
                                       python_callable=dag_extract.extract_stockcode)
    
    Extract_1 = PythonOperator(task_id = 'Crawl_Stock_Price_1',
                            python_callable=extract_price_stock_1)
    Extract_2 = PythonOperator(task_id = 'Crawl_Stock_Price_2',
                            python_callable=extract_price_stock_2)
    Extract_3 = PythonOperator(task_id = 'Crawl_Stock_Price_3',
                            python_callable=extract_price_stock_3)
    Merge_data = PythonOperator(task_id = 'Merge_Data',
                            python_callable=merge_data)
    
    ##    DAGS TRANSFORM
    
    Transform_Data = PythonOperator(task_id = 'Transform_data',
                            python_callable=transform_data_spark)
    
    
    ##      DAGS LOAD

    Create_table_sql = MySqlOperator(task_id = 'Create_Table',
                           mysql_conn_id='mysql_connection',
                           sql = sql_file['create_table'])
    Insert_File = PythonOperator(task_id = 'Insert_data',
                    python_callable=load_MySQL)


    datetime_dim = MySqlOperator(task_id = 'datetime_dim',
                           mysql_conn_id = 'mysql_connection',
                           sql = sql_file['datetime_dim'])
    price_dim = MySqlOperator(task_id = 'price_dim',
                           mysql_conn_id = 'mysql_connection',
                           sql = sql_file['price_dim'])
    OHLC_dim = MySqlOperator(task_id = 'OHLC_dim',
                           mysql_conn_id = 'mysql_connection',
                           sql = sql_file['OHLC_dim'])
    ad_dim = MySqlOperator(task_id = 'ad_dim',
                           mysql_conn_id = 'mysql_connection',
                           sql = sql_file['ad_dim'])
    vol_val_dim = MySqlOperator(task_id = 'vol_val_dim',
                           mysql_conn_id = 'mysql_connection',
                           sql = sql_file['vol_val_dim'])
    change_dim = MySqlOperator(task_id = 'change_dim',
                           mysql_conn_id = 'mysql_connection',
                           sql = sql_file['change_dim'])    
    fact_table = MySqlOperator(task_id = 'fact_table',
                           mysql_conn_id = 'mysql_connection',
                           sql = sql_file['fact_table'])  
    Extract_Stockcode >> [Extract_1,Extract_2,Extract_3] >> Merge_data  >> Transform_Data >> Create_table_sql >> Insert_File
    Insert_File >> [datetime_dim,price_dim, OHLC_dim, ad_dim, vol_val_dim, change_dim] >> fact_table
