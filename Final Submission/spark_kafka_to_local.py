import sys
import os
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# initialize spark session
spark = SparkSession  \
	.builder \
        .master("local[1]") \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	

# Reading data from "de-capstone3" topic on kafka server
orderexpersion = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","de-capstone3")  \
        .option("startingOffsets", "earliest")  \
	.load()

# select the key-value data
DFdata = orderexpersion.selectExpr("cast(key as string)","cast(value as string)")

#write data as json format on hadoop local
orderstream = DFdata  \
              .writeStream  \
              .format("json") \
              .outputMode("append")  \
              .option("path","file:///home/hadoop/cabdata/") \
              .option("checkpointLocation","file:///home/hadoop/cabdata/checkpoint/")  \
              .start()

orderstream.awaitTermination()

