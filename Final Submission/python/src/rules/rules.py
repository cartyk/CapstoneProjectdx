#importing necessary libraries
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#initialising Spark session    
spark = SparkSession  \
    .builder  \
    .appName("spark-streaming")  \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

#reading input from Kafka
orderRawData = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("startingOffsets","earliest") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "transactions-topic-verified") \
    .load()

#defining schema for a single order
jsonSchema = StructType() \
    .add("card_id", LongType()) \
    .add("member_id", LongType()) \
    .add("amount", LongType()) \
    .add("postcode", LongType()) \
    .add("pos_id", LongType()) \
    .add("transaction_dt", TimestampType()) 

   
#creating an order stream for reading data from json in kafka 
orderStream = orderRawData.select(from_json(col("value").cast("string"), jsonSchema).alias("data")).select("data.*")

query = orderStream  \
	.select("*") \
	.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime = "1 minute") \
        .start()

#indicating Spark to await termination
query.awaitTermination()
