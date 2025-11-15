import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time
username = getpass.getuser()
print(username)

# demo for managed spark table.

spark = SparkSession \
           .builder \
           .appName("debu application") \
           .config('spark.ui.port', '0') \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)

#!hadoop fs -ls /user/itv020752/warehouse/
# Found 7 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 01:23 /user/itv020752/warehouse/misgaurav_101.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:43 /user/itv020752/warehouse/misgaurav_acid.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 06:47 /user/itv020752/warehouse/misgaurav_hive.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db

spark.sql("show databases").filter("namespace like '%itv020752%'").show()
# +--------------------+
# |           namespace|
# +--------------------+
# |        itv020752_db|
# |itv020752_hivetab...|
# |itv020752_partiti...|

spark.catalog.listDatabases()[:2]
# [Database(name='0000000000000_msdian', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv009768/warehouse/0000000000000_msdian.db'),
#  Database(name='0000000000000_naveen_db', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv009240/warehouse/0000000000000_naveen_db.db')]

import re
d = []
for i in spark.catalog.listDatabases():
    if re.search(r"itv020752", i.name):
        d.append(i)

print(d)
# [Database(name='itv020752_db', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db.db'),
#  Database(name='itv020752_hivetable_savemethod', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_hivetable_savemethod.db'),
#  Database(name='itv020752_partitioning', description='', locationUri='hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_partitioning.db')]

#!hadoop fs -ls /user/itv020752/warehouse/
# Found 8 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:05 /user/itv020752/warehouse/itv020752_db.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-11 08:08 /user/itv020752/warehouse/itv020752_db_new.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 06:31 /user/itv020752/warehouse/itv020752_hivetable_savemethod.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-07-17 10:15 /user/itv020752/warehouse/itv020752_partitioning.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-15 01:23 /user/itv020752/warehouse/misgaurav_101.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-14 09:43 /user/itv020752/warehouse/misgaurav_acid.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 06:47 /user/itv020752/warehouse/misgaurav_hive.db
# drwxr-xr-x   - itv020752 supergroup          0 2025-08-13 07:17 /user/itv020752/warehouse/misgaurav_hive_new.db

df.write.format('csv').saveAsTable("itv020752_db_new.caching_demo")

#!hadoop fs -ls /user/itv020752/warehouse/itv020752_db_new.db/
# Found 2 items
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-11 08:13 /user/itv020752/warehouse/itv020752_db_new.db/caching_demo
# drwxr-xr-x   - itv020752 supergroup          0 2025-11-11 09:10 /user/itv020752/warehouse/itv020752_db_new.db/caching_demo1

spark.sql("describe extended itv020752_db_new.caching_demo").show(truncate = False)
# +----------------------------+---------------------------------------------------------------------------------------+-------+
# |col_name                    |data_type                                                                              |comment|
# +----------------------------+---------------------------------------------------------------------------------------+-------+
# |order_id                    |int                                                                                    |null   |
# |order_date                  |string                                                                                 |null   |
# |customer_id                 |int                                                                                    |null   |
# |order_status                |string                                                                                 |null   |
# |                            |                                                                                       |       |
# |# Detailed Table Information|                                                                                       |       |
# |Database                    |itv020752_db_new                                                                       |       |
# |Table                       |caching_demo                                                                           |       |
# |Owner                       |itv020752                                                                              |       |
# |Created Time                |Tue Nov 11 08:13:31 EST 2025                                                           |       |
# |Last Access                 |UNKNOWN                                                                                |       |
# |Created By                  |Spark 3.1.2                                                                            |       |
# |Type                        |MANAGED                                                                                |       |
# |Provider                    |csv                                                                                    |       |
# |Statistics                  |4289 bytes                                                                             |       |
# |Location                    |hdfs://m01.itversity.com:9000/user/itv020752/warehouse/itv020752_db_new.db/caching_demo|       |
# |Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe                                     |       |
# |InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat                                       |       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat                              |       |
# +----------------------------+---------------------------------------------------------------------------------------+-------+

spark.sql("select count(*) from itv020752_db_new.caching_demo")
# check sql tabs, it's reading from the disk, nothing will seen under storage tab on spark UI

## Scan csv itv020752_db_new.caching_demo
# number of output rows: 104
# number of files read: 1
# metadata time: 0 ms
# size of files read: 4.2 KiB


## Exchange
# shuffle records written: 1
# shuffle write time: 0 ms
# records read: 1
# local bytes read: 59.0 B
# fetch wait time: 0 ms
# remote bytes read: 0.0 B
# local blocks read: 1
# remote blocks read: 0
# data size: 16.0 B
# remote bytes read to disk: 0.0 B
# shuffle bytes written: 59.0 B

spark.sql("cache table itv020752_db_new.caching_demo")
# will seen something under storage tab on spark UI
# ID	RDD Name	                                    Storage Level	                        Cached Partitions	Fraction Cached	  Size in Memory	  Size on Disk
# 41	In-memory table itv020752_db_new.caching_demo	Disk Memory Deserialized 1x Replicated	      1	             100%	             1632.0 B	       0.0 B

spark.sql("select count(*) from itv020752_db_new.caching_demo")

## Scan csv itv020752_db_new.caching_demo
# number of output rows: 0

## Scan In-memory table itv020752_db_new.caching_demo
# number of output rows: 104

# jobs page on spark UI
## Completed Stages (2)
# sategId = 15, Tasks: Succeeded/Total = 1/1, input, Output, Shuffle Read = 59B, Shuffle Write
# sategId = 14, Tasks: Succeeded/Total = 1/1, input = 1632.0 B, Output, Shuffle Read, Shuffle Write = 59B

#  it'll show you a process_local in the first stage, ## means heading
## Tasks (1)
# Status = SUCCESS
# Locality Level = PROCESS_LOCAL
# host = w02.itversity.com

# in the second stage, it'll show you a NODE_LOCAL
## Tasks (1)
# Status = SUCCESS
# Locality Level = NODE_LOCAL
# host = w02.itversity.com


spark.sql("select count(distinct order_id ) from itv020752_db_new.caching_demo")

# jobs page on spark UI
## Completed Stages (2)
# sategId = 21, Tasks: Succeeded/Total = 1/1, input, Output, Shuffle Read = 11.2KB, Shuffle Write
# sategId = 20, Tasks: Succeeded/Total = 200/200, input, Output, Shuffle Read = 5KB, Shuffle Write = 11.2B
# sategId = 19, Tasks: Succeeded/Total = 1/1, input = 1632.0 B, Output, Shuffle Read, Shuffle Write = 5KB


#  it'll show you a process_local in the first stage, ## means heading, open stageId = 19 link
## Tasks (1)
# Status = SUCCESS
# Locality Level = PROCESS_LOCAL
# host = w02.itversity.com

#  it'll show you a process_local in the second stage, ## means heading, open stageId = 20 link, you'll see 200 records
## Tasks (1)
# Status = SUCCESS
# Locality Level = PROCESS_LOCAL OR NODE_LOCAL
# host = w02.itversity.com OR w01.itversity.com
# Shuffle write size / records = 56B/1 # for PROCESS_LOCAL, only Shuffle write size / records is there
# Shuffle read size / records = some_value, Shuffle write size / records = some_value, for NODE_LOCAL

#  it'll show you a node_local in the third stage, ## means heading, open stageId = 21 link
## Tasks (1)
# Status = SUCCESS
# Locality Level = NODE_LOCAL
# host = w02.itversity.com

# reading from the InMemoryTableScan, it'll show you a process_local, again it will hit the cache
## Scan csv itv020752_db_new.caching_demo
# number of output rows: 0

## Scan In-memory table itv020752_db_new.caching_demo
# number of output rows: 104

spark.sql("select order_status, count(*) from itv020752_db_new.caching_demo group by 1").show()

spark.sql("insert into itv020752_db_new.caching_demo values (1111, '2023-05-12', 22222, 'BOOKED')").show()

#!hadoop fs -ls -h /user/itv020752/warehouse/itv020752_db_new.db/caching_demo

# Found 3 items, this will create a new file under
# -rw-r--r--   3 itv020752 supergroup          0 2025-11-11 10:17 /user/itv020752/warehouse/itv020752_db_new.db/caching_demo/_SUCCESS
# -rw-r--r--   3 itv020752 supergroup         29 2025-11-11 10:17 /user/itv020752/warehouse/itv020752_db_new.db/caching_demo/part-00000-5e6fcc86-24da-477f-bb91-db6548183400-c000.csv
# -rw-r--r--   3 itv020752 supergroup      4.2 K 2025-11-11 08:13 /user/itv020752/warehouse/itv020752_db_new.db/caching_demo/part-00000-89dfe0e3-733f-43f0-b22c-9ecb7c165f1b-c000.csv

spark.sql("select order_status, count(*) from itv020752_db_new.caching_demo group by 1").show()
# this time, it'll take a lot of time to run it, even though we cache the data. so here, it's refreshing the cache data.
# and it will show you a node_local. 

# if we run the same line again, then it will hit the cache, it will show you a process_local.
spark.sql("select order_status, count(*) from itv020752_db_new.caching_demo group by 1").show()

process local while reading the cache data is ; ➡️ Same machine, same executor, same process.

node local while reading the cache data is : ➡️ Same machine, but different process or source.

Node-local does NOT involve multiple nodes.
It means the task and the data are on the same node.

Process-local also uses only one machine (even stricter: one JVM process).

Data is in the same JVM process where the task is running.

Example: process local 
You cached an RDD/DataFrame partition in memory on an executor, and Spark schedules the next task on the same executor + same JVM process.

example: node local 
Slightly slower than process-local.

Data is on the same machine, but in a different process.

Example:

The data might be in HDFS block on same node

Or another executor’s memory on same node

Or another JVM (like an external shuffle service)
