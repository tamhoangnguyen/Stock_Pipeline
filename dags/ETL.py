from urllib.request import Request,urlopen
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functions import *
import pandas as pd
import json
import os



class Extract:
     def __init__(self):
          self.header = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
                    'Connection' : 'keep-alive'
                    }
          #self.path_company = 'https://api-dulieu.mbs.com.vn/api/OverviewMarket/GetAutoCompleteCompanyAndIndex?languageId=1'
          self.path_stockcode = "/home/leader/Documents/airflow-environment/dags/StockCode.csv"
          self.path_web = "https://finfo-api.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{}~date:gte:{}~date:lte:{}&size=9990&page=1"
          self.miss_data = []
          self.data = []
     def extract_company(self,path_company):
          req = Request(path_company,headers=self.header)
          x = urlopen(req,timeout=10).read()
          req.add_header("Authorization", "Basic %s" % "ABCZYXX")
          json_x = json.loads(x)['Data']
          df = pd.DataFrame(json_x)
          return df
     def extract_lenstockcode(self):
          df = pd.read_csv(self.path_stockcode)
          self.code = list(df['Stock_Code'].values)
          return len(self.code)
     def extract_stockcode(self):
          df = pd.read_csv(self.path_stockcode)
          self.code = list(df['Stock_Code'].values)
          return self.code
     def crawl_data(self ,code ,to_date : str, from_date : str, start : int, end : int):
          self.code = code[start:end]
          print(f"Prepare crawl data {from_date} !")
          for stock_code in self.code:
               url = self.path_web.format(stock_code, from_date, to_date)
               try:
                    req = Request(url, headers= self.header)
                    x = urlopen(req, timeout=10).read()
                    req.add_header("Authorization", "Basic %s" % "ABCZYXX")
                    json_x = json.loads(x)['data']
                    if len(json_x) != 0:
                         json_x = json_x[0]
                         print(f"Crawl data {json_x['code']} is success !")
                         self.data.append(json_x)
                    else:
                         print(f"Don't have data {stock_code} in {from_date} !")
                         self.miss_data.append(stock_code)
               except Exception as e:
                    print("Can't connect the web !")
                    print(e)
                    return None
          if len(self.data) == 0:
               return None
          return self.data # Return list
        
class Transform(Extract):
     def __init__(self):
          super().__init__()
          findspark.init()
          self.target = '/home/leader/Documents/airflow-environment/data_dim/'
     def create_spark(self):
          try:
               spark = SparkSession \
                    .builder \
                    .config('spark.driver.host','localhost') \
                    .appName("Pyspark Structured Stock Price 2013 - 2023") \
                    .master("local[2]") \
                    .getOrCreate()
               return spark
          except Exception as e:
               print(e)
               return None
          
          
     def transform(self,path): # transform function return path in dags, if function only return one output , need return None
          spark = self.create_spark()
          df = spark.read.csv(path,inferSchema = True , header = True)
          df = df.withColumn('date_string',date_format(col('date'),"yyyy-MM-dd")) \
          .withColumn('year',split(col('date_string'),'-')[0].cast('int')) \
          .withColumn('month',split(col('date_string'),'-')[1].cast('int')) \
          .withColumn('day',split(col('date_string'),'-')[2].cast('int')) \
          .withColumn('date_update',split(col('time'),' ')[0]) \
          .withColumn('year_update',split(col('date_string'),'-')[0].cast('int')) \
          .withColumn('month_update',split(col('date_string'),'-')[1].cast('int')) \
          .withColumn('day_update',split(col('date_string'),'-')[2].cast('int')) \
          .withColumn('time',split(col('time'),' ')[1]) \
          .withColumn('hours',split(col('time'),':')[0].cast('int')) \
          .withColumn('minutes',split(col('time'),':')[1].cast('int')) \
          .withColumn('seconds',split(col('time'),':')[2].cast('int')) \
          .withColumn('date_update',col('date_update').cast('date'))
          
          # Return to pandas DataFrame to convert csv file and put data into Bigquery
          # If have a google cloud storage ( have cost ) , we don't need to convert pandas.DataFrame, \
               # pyspark DataFrame can put data into Bigquery directly through google cloud storage with Example: 
                    # bucket = "[bucket]"
                    # spark.conf.set('temporaryGcsBucket', bucket) 
                    # df.write.format('bigquery').option('table', ( 'project_id.dataset_id.tablename')).mode("overwrite").save()

          df = df.toPandas()
          column = ['code','stock_date','time_update','floor','typeStock',
              'basicPrice','ceilingPrice','floorPrice',
              'open_col','high_col','low_col','close_col','average_col',
              'ad_open','ad_high','ad_low','ad_close','adaverage',
              'nmVolumne','nmValue','ptVolumne','ptValue',
              'changecol','adChange','pctChange','date_string',
              'year_col','month_col','day_col','date_update','year_update','month_update','day_update','hours_update','minutes_update','seconds_update']
    
          col_names = dict(zip(df.columns,column))
          df = df.rename(columns=col_names)
          path = self.target + 'full_data.csv'
          print('Transform data is success !')
          return df, path
          
class Load(Transform):
     def __init__(self):
          super().__init__()
          self.project_id = None
          self.project_id = 'my-project-23-02-01'
          self.my_dataset = 'Pipelines_Airflow_Example'
          os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/leader/Documents/airflow-environment/api.json" 
          credentials, project_id = default()
     
     def load_data_into_Bigquery(self):
          for name_table, df in self.data.items():
               table_id = "{}.{}.{}".format(self.project_id,self.my_dataset,name_table)
               try:
                    df.to_gbq(
                              destination_table=table_id,
                              project_id= self.project_id,
                              if_exists="append",
                              )
                    print(f'Upload table {name_table} is sucess !')
               except Exception as e:
                    print(e)
                    print('Fail !')
                    continue
               
     def load_data_into_MySQL(self,path : str,table_name :str): 
          connect_sql = Batch_Processing_data()
          data = pd.read_csv(path)
          connect_sql.save_to_my_sql(data,table_name)
               
