import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)

# demo for managed spark table.


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

   df = spark.read.csv("/Users/gauravmishra/Desktop/cachingDemo/Datasets/orders.csv", header=True, inferSchema=True)

   spark.sql("create database itv016380_db")
   # this will create a db as a folder,  under /user/{username}/warehouse/itv016380.db, inside this folder our table will be created.
   # we called this table as a managed table

   df.write.format('csv').saveAsTable("itv016380_db.caching_demo")

   # you will be able to see under
   # !hadoop fs -ls /user/itv016380/warehouse

   spark.sql("describe extended itv016380_db.caching_demo")

   spark.sql("select count(*) from itv016380_db.caching_demo")
   # check sql tabs, it's reading from the disk

   spark.sql("cache itv016380_db.caching_demo")

   spark.sql("select count(*) from itv016380_db.caching_demo")
   # reading from the InMemoryTableScan, it'll show you a process_local

   spark.sql("select count(distinct id) from itv016380_db.caching_demo")
   # reading from the InMemoryTableScan, it'll show you a process_local, again it will hit the cache

   spark.sql("uncache itv016380_db.caching_demo") # uncache the table

   spark.sql("select order_status, count(*) from itv016380_db.caching_demo group by 1").show()

   spark.sql("insert into itv016380_db.caching_demo values (1111, '2023-05-12', 22222, 'BOOKED')").show()

   # this will create a new file under !hadoop fs -ls /user/itv016380/warehouse/itv016380_db.db/caching_demo/
   # this data (1111, '2023-05-12', 22222, 'BOOKED') came as a new file under: !hadoop fs -ls /user/itv016380/warehouse/itv016380_db.db/caching_demo/

   spark.sql("select order_status, count(*) from itv016380_db.caching_demo group by 1").show()
   # this time, it'll take a lot of time to run it, even though we cache the data. so here, it's refreshing the cache data.
   # and it will show you a node_local. 

   # if we run the same line again, then it will hit the cache, it will show you a process_local. 
