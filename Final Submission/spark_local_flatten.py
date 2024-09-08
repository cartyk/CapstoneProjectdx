import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

spark = SparkSession  \
	.builder  \
	.appName("carrideanalysis")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	
	
orderexpersion = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","de-capstone3")  \
    .option("startingOffsets", "latest")  \
	.load()

# defining schema
dfSchema = StructType()\
     .add("customer_id",LongType()) \
     .add("app_version",StringType()) \
     .add("OS_version",StringType()) \
     .add("lat",DoubleType()) \
     .add("lon",DoubleType()) \
     .add("page_id",StringType()) \
     .add("button_id",StringType()) \
     .add("is_button_click",BooleanType()) \
     .add("is_page_view",BooleanType()) \
     .add("is_scroll_up",BooleanType()) \
     .add("is_scroll_down",BooleanType()) \
     .add("timestamp", TimestampType()) \

# creating dataframe from input data after applying the schema

orderDF = orderexpersion.select(from_json(col("value").cast("string"),dfSchema).alias("clickdata")).select("clickdata.*")

# Writing the input data(from kafka afyer transformation) into console

orderstream = orderDF\
    .select("*")\
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
    .option("truncate","false")\
    .trigger(processingTime="1 minute")\
	.start()


orderstream.awaitTermination()

