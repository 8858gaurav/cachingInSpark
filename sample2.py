import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)


if __name__ == '__main__':
   print("creating spark session")


   spark = SparkSession \
           .builder \
           .appName("debu application") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .config("spark.driver.bindAddress","localhost") \
           .config("spark.ui.port","4041") \
           .master("local[*]") \
           .getOrCreate()
   spark.sparkContext.setLogLevel('WARN')

   df = spark.read.csv("/Users/gauravmishra/Desktop/cachingDemo/Datasets/Lung Cancer.csv", header=True, inferSchema=True)

   print(df.distinct().count())
   # distinct will create a 200 partitions by default, it's a wide transaformations.

   cached_df = df.cache()

   cached_df.count()
   # on the basis fo line 32, this time it will stored the data under cache from the csv file. it will not read the data from cache.

   print(cached_df.distinct().count())
   # this time, line no 34, it will hit the data from the cache, it will read the table from the InMemoryTableScan

   print(df.count())
   # df.count() will also hit the cache, it read the data from the InMemoryTableScan, same as cached_df.distinct().count()

   time.sleep(10240)