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
           .config("spark.sql.shuffle.partitions", 3) \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .config("spark.driver.bindAddress","localhost") \
           .config("spark.ui.port","4041") \
           .master("local[*]") \
           .getOrCreate()
   spark.sparkContext.setLogLevel('WARN')

   df = spark.read.csv("/Users/gauravmishra/Desktop/cachingDemo/Datasets/orders.csv", header=True, inferSchema=True)
   # the files size is 89 MB here.
   print(df.take(2))
   # [Row(order_id=1, order_date=datetime.datetime(2013, 7, 25, 0, 0), order_customer_id=11599, order_status='CLOSED'), Row(order_id=2, order_date=datetime.datetime(2013, 7, 25, 0, 0), order_customer_id=256, order_status='PENDING_PAYMENT')]
   df = df.withColumn("updated", expr("case when order_status == 'CLOSED' THEN order_id*2 ELSE order_id END"))
   # [Row(order_id=1, order_date=datetime.datetime(2013, 7, 25, 0, 0), order_customer_id=11599, order_status='CLOSED', updated=1), Row(order_id=2, order_date=datetime.datetime(2013, 7, 25, 0, 0), order_customer_id=256, order_status='PENDING_PAYMENT', updated=2)]

   df = df.withColumnRenamed("updated", "updated_new")
   # [Row(order_id=1, order_date=datetime.datetime(2013, 7, 25, 0, 0), order_customer_id=11599, order_status='CLOSED', updated_new=1), Row(order_id=2, order_date=datetime.datetime(2013, 7, 25, 0, 0), order_customer_id=256, order_status='PENDING_PAYMENT', updated_new=2)]

   # if I call, df.show() 2 times here, it will read the data from the disk two times. 
   # caching - keeping something in memory & not going to the disk again. 

   df2 = df.cache()
   # first time, it has to read from the disk
   df2.show()
   # second time, it will read from the memory (cache), till here it'll cache just 1 partitions, showing 20 records only, then why
   # we need to cache everything
   df2.show()
   # now to cache everything call collect or count any function on the complete dataset, then check spark web ui storage tabs.
   df2.count()
   time.sleep(12200)