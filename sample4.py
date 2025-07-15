import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)

# demo for ext spark table.


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

   spark.sql("create database itv016380_db")

   spark.sql("create table itv016380_db.caching_demo_ext (order_id long, order_date string, customer_id long, order_status string) using csv location '/user/itv016380/orders'")

   spark.sql("describe extended itv016380_db.caching_demo_ext").show()

   spark.sql("cache table itv016380_db.caching_demo_ext")

   spark.sql("insert into itv016380_db.caching_demo_ext values (1111, '2023-05-12', 22222, 'BOOKED')").show()

    # this will create a new file under !hadoop fs -ls /user/itv016380/orders/itv016380_db.db/caching_demo_ext/
   # this data (1111, '2023-05-12', 22222, 'BOOKED') came as a new file under: !hadoop fs -ls /user/itv016380/orders/itv016380_db.db/caching_demo/

   spark.sql("select count(*) from itv016380_db.caching_demo_ext").show()
   # this time, it will refresh the cache, it'll not hit the cache. you will not see process_local there. 

   spark.sql("select count(*) from itv016380_db.caching_demo_ext").show()
   # if we run this again, it will hit the cache. you will see process_local there. 

   #!hadoop fs -put orders_incremental.csv /user/itv016380/orders: placing the file in hadoop location from locally.

   # run the same query
   spark.sql("select count(*) from itv016380_db.caching_demo_ext").show()
   # it'll still dealing with the old data, what we have till line no 42. we need to refresh the table manually.

   spark.sql("refresh itv016380_db.caching_demo_ext")

   # run the same query
   spark.sql("select count(*) from itv016380_db.caching_demo_ext").show()
   # now it'll deal with the fresh data. 

   #!hadoop fs -rm /user/itv016380/orders/orders_incremental.csv

    # run the same query
   spark.sql("select count(*) from itv016380_db.caching_demo_ext").show()
   # it'll still dealing with the old data, what we have till line no 54. we need to refresh the table manually.

   spark.sql("refresh itv016380_db.caching_demo_ext")

   # when we insert the data using insert command, then spark will know that the cache is invalidates, and in nest steps
   # it will refresh the data. 

   # but when we deal with adding or removing files in backend, then spark can't track it, and we have to refresh the table manually. 

   # insert command will work for managed and ext spark tables both
   # but placing a files will work for only ext tables. 
   # in managed table, we can't place the files manually, since managed table data is handeled by the Hive.
