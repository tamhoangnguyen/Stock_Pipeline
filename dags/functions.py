from urllib.request import Request, urlopen
from pyspark.sql.functions import DataFrame
from selenium import webdriver
from kafka import KafkaProducer
from json import dumps
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.auth import default
from webdriver_manager.chrome import ChromeDriverManager
import mysql.connector
import json
import pandas as pd
import threading
import findspark
import time
import os
import sqlalchemy

class Crawl_Data:
    def get_element(driver,name,link,count,data):
        try:
            x = driver.find_element('xpath',link.format(count)).text
            data[f'{name}'].append(x)
        except:
            data[f'{name}'].append(None)  
    def crawl_stock_code(self):
        driver = webdriver.Chrome(ChromeDriverManager().install())
        path = 'https://dulieu.mbs.com.vn/vi/OverviewMarket'
        driver.get(path)
        driver.find_element("xpath","/html/body/div[1]/div[2]/div[2]/div[3]")

        #Link của từng loại dữ liệu
        Stock_code_link = '/html/body/div[1]/div[2]/div[2]/div[3]/div[3]/table/tbody/tr[{}]/td[2]'
        Name_link = '/html/body/div[1]/div[2]/div[2]/div[3]/div[3]/table/tbody/tr[{}]/td[3]'
        Industry_link = '/html/body/div[1]/div[2]/div[2]/div[3]/div[3]/table/tbody/tr[{}]/td[4]'
        Exchange_link = '/html/body/div[1]/div[2]/div[2]/div[3]/div[3]/table/tbody/tr[{}]/td[5]'
        KLCPNY_link = '/html/body/div[1]/div[2]/div[2]/div[3]/div[3]/table/tbody/tr[{}]/td[6]'
        data = {
            'Stock_Code' : [],
            'Name_Company' : [],
            'Industry' :[],
            'Exchange' : [],
            'KLCPNY': []
        }   
        for i in range(3,1607):
            data1 = threading.Thread(name='Stock_Code',target= self.get_element(driver,'Stock_Code',Stock_code_link,i,data))
            data2 = threading.Thread(name='Name_Company',target= self.get_element(driver,'Name_Company',Name_link,i,data))
            data3 = threading.Thread(name='Industry',target= self.get_element(driver,'Industry',Industry_link,i,data))
            data4 = threading.Thread(name='Exchange',target= self.get_element(driver,'Exchange',Exchange_link,i,data))
            data5 = threading.Thread(name='KLCPNY',target= self.get_element(driver,'KLCPNY',KLCPNY_link,i,data))
            print(f'Data {i - 2} is success !')
        data1.start(),data2.start(),data3.start(),data4.start(),data5.start()
        print('Finish !')
        result = pd.DataFrame(data)
        result.to_csv("E:\\Project\\StockCode.csv",index = False)
        return result

    def extract_stockcode(self):
        #df = crawl_stock_code()
        df = pd.read_csv("E:\\Project\\dags\\StockCode.csv")
        data = list(df['Stock_Code'].values)
        return data

    def crawl_data(stock_code,to_date,from_date): # Trả về data của tất cả dữ liệu
        url = "https://finfo-api.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{}~date:gte:{}~date:lte:{}&size=9990&page=1"\
            .format(stock_code, from_date, to_date)
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Connection' : 'keep-alive'
        }
        try:
            req = Request(url, headers=header)
            x = urlopen(req, timeout=10).read()
            req.add_header("Authorization", "Basic %s" % "ABCZYXX")
            json_x = json.loads(x)['data'] # --> return list of dict
            if len(json_x) == 0:
                print(f"Don't have data {stock_code} in {from_date}")
            else:
                if len(json_x) == 1: # Data mỗi ngày thì trả về 1 phần tử dict trong mảng
                    json_x = json_x[0]
                    print(f"Crawl data {stock_code} is success !")
                else: 
                    print(f"Crawl multi data {stock_code} is success")
        except Exception as e:
            print("Can not connect this web !")
            print(e)
        return json_x

class MySQL:
    def __init__(self) -> None:
        self.mysql_host_name = "localhost"
        self.mysql_port_no = "3306"
        self.mysql_driver_class = "com.mysql.jdbc.Driver"
        self.mysql_database_name = "stock_price"
        self.mysql_user_name = "leader"
        self.mysql_password = 'T4md3ptr~i'
        self.mysql_jdbc_url = "jdbc:mysql://" + self.mysql_host_name + ":" + self.mysql_port_no + "/" + self.mysql_database_name
        self.connect_database() # When we call the class, connect database auto activate
    def connect_database(self):
        self.conn = mysql.connector.connect(
            host=self.mysql_host_name,
            user=self.mysql_user_name,
            password=self.mysql_password,
            database=self.mysql_database_name
        )
        self.curr = self.conn.cursor()
    def create_new_table(self,mysql_table_name : str ,statements_createtable: None):
        # Example: 
        self.curr.execute(f"""CREATE TABLE IF NOT EXISTS {mysql_table_name} ( id int AUTO_INCREMENT KEY,code TEXT,date TIMESTAMP,time TEXT,floor TEXT,type TEXT, 
                          basicPrice DOUBLE ,ceilingPrice DOUBLE ,floorPrice DOUBLE,open DOUBLE, 
                          high DOUBLE, low DOUBLE, close DOUBLE, average DOUBLE,
                          adOpen DOUBLE, adHigh DOUBLE, adLow DOUBLE, adClose DOUBLE,adAverage DOUBLE,
                          nmVolume DOUBLE, nmValue DOUBLE, ptVolume DOUBLE, ptValue DOUBLE, 
                          changeCol DOUBLE, adChange DOUBLE, pctChange DOUBLE)""")
        #self.curr.execute(statements_createtable.format(mysql_table_name))
        print('Create table is success !')
        self.table = mysql_table_name
        
    def save_to_my_sql(self,data,id):
        print(f"Message {id + 1} is prepare !")
        db_credentials = {
            'users' : 'root',
            'password' : self.mysql_password,
            'driver': self.mysql_driver_class
        }
        data \
            .write \
            .jdbc( url = self.mysql_jdbc_url,
                table = self.table,
                mode = 'append',
                properties = db_credentials)
        print(f"Save Message {id + 1} to mysql is success !")
    
    # Query data
    def query_data(self,query):
        data = []
        self.curr.execute(query)
        result = self.curr.fetchall()
        for x in result:
            data.append(x)
        return data
    
    def close_conn_cur(self):
        self.curr.close()
        self.conn.close()
        
    def transform_data_to_DataFrame(self,sql_statement,columns):
        data = self.query_data(sql_statement)
        df = pd.DataFrame(data,columns=columns)
        return df

    def close_conn_cur(self):
        self.curr.close()
        self.conn.close()
        

class Bigquery_API:
    def __init__(self) -> None:
        self.project_id = 'my-project-23-02-01'
        self.dataset_id = 'Pipelines_Airflow'
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "E:\\Project\\api.json" # Access API in GCP
        credentials, project_id = default()
        self.client = bigquery.Client(credentials=credentials,project=project_id)
    def get_data(self,query : str):
        # Query Example: "SELECT distinct * FROM {project_id}.{dataset_id}.{name_table} WHERE date = '2023-08-03 00:00:00 UTC'"
        data = []
        result = self.client.query(query)
        # Result will return 2 value : 
        # - Values return the row values have type tuple 
        # - Field_to_index return dict a mapping from schema field names to indexes
        for row in result: 
            data.append(row.values())
        print("Get data in bigQuery is success !")
        return data # Return array
    def load_bigquery(self, data: DataFrame, table_name : str):
        table_id = "{}.{}.{}".format(self.project_id,self.dataset_id,table_name)
        data.to_gbq(
                    destination_table=table_id,
                    project_id= self.project_id,
                    if_exists="append",
                )
        print(f"Table data {table_name} is upload Bigquery success !")
        time.sleep(1)

class Streaming_data(MySQL,Crawl_Data):
    def __init__(self) -> None:
        super().__init__()
        SCALA_VERSION = '2.12'
        SPARK_VERSION = '3.1.2'
        os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'
        findspark.init()
        self.BootStrapServer = 'localhost:9092'
        self.topic_name = 'Securities_Pipeline_Data'
        self.connect_kafka()

    def connect_kafka(self):
        self.producer = KafkaProducer(bootstrap_servers = [''],value_serializer = lambda x: dumps(x).encode('utf-8'))
        
    def send_item(self, metadata):      
        for data in metadata:
            self.producer.send(self.topic_name, value = data)
        print(f"Send data {data['code']} is success !")
        time.sleep(1)
             
    def run_streaming(self):
        Stock_Code_data = self.extract_stockcode()
        to_date = "2023-07-26"
        from_date = "2000-01-01"
        for x in Stock_Code_data:  # Get Stock Code and put crawl_data
            metadata = self.crawl_data(x,to_date,from_date)
            self.send_item(metadata)
    
    def extract_data(self):
        spark = SparkSession \
            .builder \
            .appName("Pyspark Structured Stock Price 2013 - 2023") \
            .master("local[*]") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers",self.BootStrapServer) \
            .option("subscribe",self.topic_name) \
            .option("startingOffsets","latest") \
            .load()
        
        df.printSchema()
        df1 = df.selectExpr("CAST(value as STRING)","timestamp")
        schema = "code STRING,date TIMESTAMP,time STRING,floor STRING \
            ,type STRING, basicPrice DOUBLE ,ceilingPrice DOUBLE ,floorPrice DOUBLE \
            ,open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, average DOUBLE \
            ,adOpen DOUBLE, adHigh DOUBLE, adLow DOUBLE, adClose DOUBLE,adAverage DOUBLE \
            ,nmVolume DOUBLE, nmValue DOUBLE, ptVolume DOUBLE, ptValue DOUBLE, change DOUBLE, adChange DOUBLE, pctChange DOUBLE"
        df2 = df1 \
            .select(from_json(col("value"),schema) \
            .alias("records"))
        
        full_data = df2.select("records.*")
        
        full_data \
            .writeStream \
            .trigger(processingTime ='5 seconds') \
            .outputMode("update") \
            .option("truncate","false") \
            .foreachBatch(self.save_to_my_sql) \
            .start()
        
        
        detail_write_stream = full_data \
            .writeStream \
            .trigger(processingTime ='5 seconds') \
            .outputMode("update") \
            .option("truncate","false") \
            .format("console") \
            .start()
        detail_write_stream.awaitTermination()

class Batch_Processing_data(MySQL,Crawl_Data,Bigquery_API): 
    def __init__(self) -> None:
        super().__init__()
        self.conn = sqlalchemy.create_engine(f'mysql+mysqlconnector://{self.mysql_user_name}:{self.mysql_password}@{self.mysql_host_name}/{self.mysql_database_name}')
    def crawl_data(stock_code : list, to_date, from_date,start,end,data : list,miss_data: list): # Trả về data mỗi ngày 
        for i in range(start,end): 
            url = "https://finfo-api.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{}~date:gte:{}~date:lte:{}&size=9990&page=1"\
                .format(stock_code[i], from_date, to_date)
            header = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
                'Connection' : 'keep-alive'
            }
            try:
                req = Request(url, headers=header)
                x = urlopen(req, timeout=10).read()
                req.add_header("Authorization", "Basic %s" % "ABCZYXX")
                json_x = json.loads(x)['data'] # --> return list of dict
                if len(json_x) == 0:
                    print(f"Don't have data {stock_code} in {from_date}")
                    miss_data.append(json_x)
                else:
                    json_x = json_x[0]
                    print(f"Crawl data {stock_code} is success !")
                    data.append(json_x)
            except Exception as e:
                print("Can not connect this web !")
                print(e)
                data = []
                break
        return data
    
    def save_to_my_sql(self, data : DataFrame ,table_name: str):
        data.to_sql(con=self.conn,name = table_name,if_exists='append',index =False)
    
    # def load_data_bigquery_into_sql(self,table_name,columns): # input table name,columns
    #     data = self.get_data() # Functions of Bigquery API
    #     df = pd.DataFrame(data,columns)
    #     self.save_to_my_sql(df,table_name) # Functions of Batch_Processing_data
    #     print('Upload data into MySQL is success ! ')
