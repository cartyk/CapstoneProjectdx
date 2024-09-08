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
    .option("startingOffsets","latest") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "transactions-topic-verified") \
    .load()

#defining schema for a single order
jsonSchema = StructType() \
    .add("card_id", StringType()) \
    .add("member_id", StringType()) \
    .add("amount", StringType()) \
    .add("postcode", StringType()) \
    .add("pos_id", StringType()) \
    .add("transaction_dt", StringType()) 

   
#creating an order stream for reading data from json in kafka 
orderStream = orderRawData.selectExpr('CAST(value AS STRING)').select(from_json('value', jsonSchema).alias("data")).select("data.*")

query = orderStream  \
	.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

#indicating Spark to await termination
query.awaitTermination()
