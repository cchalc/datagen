# Databricks notebook source
pip install faker

# COMMAND ----------

# MAGIC %md 
# MAGIC ### configure

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### streaming data frame

# COMMAND ----------

import os
import time
from datetime import timedelta, datetime

from pyspark.sql.functions import count, when, isnan, col, lit, countDistinct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DoubleType, LongType



# COMMAND ----------

schema_sensor = StructType([
  StructField("GPSData", ArrayType(
    StructType([
      StructField("SnappedLongitude", FloatType(), True),
      StructField("GPSLongitude", FloatType(), True),
      StructField("TimeStamp", DoubleType(), True),
      StructField("GPSHeading", IntegerType(), True),
      StructField("GPSAltitude", IntegerType(), True),
      StructField("GPSSpeed", FloatType(), True),
      StructField("Deviation", FloatType(), True),
      StructField("FromNodeID", LongType(), True),
      StructField("WayID", LongType(), True),
      StructField("SnappedLatitude", FloatType(), True),
      StructField("ToNodeID", LongType(), True),
      StructField("HorizontalAccuracy", IntegerType(), True),
      StructField("GPSLatitude", FloatType(), True),
      StructField("VerticalAccuracy", IntegerType(), True),
      ])
  ), True),
  StructField("DataAlgorithmVersion", StringType(), True),
  StructField("TripID", StringType(), True),
  StructField("StartTime", DoubleType(), True),
  StructField("CompanyID", StringType(), True),
  StructField("StartTimeZone", StringType(), True),
  StructField("DriverID", StringType(), True),
  StructField("EndTime", DoubleType(), True),
  StructField("EndTimeZone", StringType(), True),
])

# COMMAND ----------

import datetime
from datetime import date, timedelta
import random
import json
from faker import Faker

# COMMAND ----------

# config and constraints
Faker.seed(0)
fake = Faker()

start_date = date(2022, 1, 1)
num_days = 25
dates = [start_date + timedelta(n) for n in range(num_days)]
end_date = date(2022, 1, num_days)
num_customers = 20

import pytz
tz_list = pytz.all_timezones
can_tz = list(filter(lambda k: 'Canada' in k, tz_list))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import string

def returnRandomJson():
  import datetime
  import random
  import json
  
  jsonObject={
    "GPSData": [
      {
        "SnappedLongitude": fake.pyfloat(min_value=-180, max_value=180),
        "GPSLongitude": fake.pyfloat(min_value=-180, max_value=180),
        "TimeStamp": fake.iso8601(),
        "GPSHeading": fake.random_int(0, 360),
        "GPSAltitude": fake.random_int(0, 500),
        "GPSSpeed": fake.pyfloat(min_value=0, max_value=250),
        "Deviation": fake.pyfloat(),
        "FromNodeID": fake.pyfloat(),
        "WayID": fake.pyfloat(),
        "SnappedLatitude": fake.pyfloat(min_value=-90, max_value=90),
        "ToNodeID": fake.pyfloat(),
        "HorizontalAccuracy": fake.pyint(),
        "GPSLatitude": fake.pyfloat(min_value=-90, max_value=90),
        "VerticalAccuracy": fake.pyint(min_value=0, max_value=100),
      }
    ],
    "DataAlgorithmVersion": fake.pystr(),
    "TripID": fake.unique.random_int(),
    "StartTime": fake.iso8601(),
    "CompanyID": ''.join(random.choice('abcdefghijklmnopqrtzusv') for i in range(5)).upper(),
    "StartTimeZone": random.choice(can_tz),
    "DriverID": fake.pystr(),
    "EndTime": fake.iso8601(),
    "EndTimeZone": random.choice(can_tz),
  }
  return(json.dumps(jsonObject))

returnRandomJsonUDF = udf(returnRandomJson, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Set the schema for event hub. I will comment out the event hub related fields for the test

# COMMAND ----------

schemaEventHub = StructType([
  StructField("body", BinaryType(), True),              # Our JSON payload
  # StructField("partition", StringType(), True),         # The partition from which this record is received
  # StructField("offset", StringType(), True),            # The position of this record in the corresponding EventHubs partition
  # StructField("sequenceNumber", LongType(), True),      # A unique identifier for a packet of data (alternative to a timestamp)
  # StructField("enqueuedTime", TimestampType(), True),   # Time when data arrives
  # StructField("publisher", StringType(), True),         # Who produced the message
  # StructField("partitionKey", StringType(), True),      # A mechanism to access partition by key
  # StructField("properties", MapType(StringType(), StringType()), True),           # Extra properties
  # StructField("systemProperties", MapType(StringType(), StringType()), True)       # Extra properties
])

# COMMAND ----------

streamingRateDF = (
    spark.readStream.format(  # Returns DataStreamReader
        "rate"
    )  # Rate source (for testing) - generates data at the specified number of rows per second
    .option("rowsPerSecond", 1)  # How many rows should be generated per second.
    .load()  # schema is 'timestamp TIMESTAMP, value LONG'
)

# COMMAND ----------

streamingEventHubDF = (streamingRateDF
  .withColumn("body", returnRandomJsonUDF().cast("binary"))
  # .withColumn("partition", lit("0"))
  # .withColumn("offset", col("value")+100)
  # .withColumn("sequenceNumber", col("value"))
  # .withColumn("enqueuedTime", col("timestamp"))
  # .withColumn("publisher", lit("null"))
  # .withColumn("partitionKey", lit("null"))
  # .withColumn("properties", lit("{}"))
  # .withColumn("systemProperties", lit("{}"))
  # .drop("timestamp", "value")
)

# COMMAND ----------

streamingQuery = (streamingEventHubDF              # Start with the "streaming" DataFrame
  .writeStream                                     # Get the DataStreamWriter
  .queryName("memorySinkStream")                   # Name the query
  .trigger(processingTime="5 seconds")             # Configure for a 2-second micro-batch
  .format("memory")                                # The output is stored in memory as an in-memory table
  .outputMode("append")                            # Write only new data to the "memory"
  .start()                                         # Start the job
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM memorySinkStream

# COMMAND ----------

streamingProcessedDF = (streamingEventHubDF
  .select(
    # year(col("enqueuedTime")).alias("year"),
    # month(col("enqueuedTime")).alias("month"),
    get_json_object(col("body").cast("string"),"$.eventType").alias("event"),
    col("body").cast("string").alias("payload")
  )
)

# COMMAND ----------

# s3://one-env-uc-external-location/shared_location/cchalc/datagen/sensor/

workingDir = "s3://one-env-uc-external-location/shared_location/cchalc/datagen/sensor"
workingDir = f"{cloud_storage_path}"
outputPathBronze =     workingDir + "/bronze.delta"
checkpointPathBronze = workingDir + "/bronze.checkpoint"


streamingQuery = (streamingProcessedDF                 # Start with the "streaming" DataFrame
  .writeStream                                         # Get the DataStreamWriter
  .queryName("deltaSinkStream")                        # Name the query
  .trigger(processingTime="5 seconds")                 # Configure for a 5-second micro-batch
  .format("delta")                                     # The output is stored in a delta table
  # .partitionBy("event", "year", "month")
  .outputMode("append")                                # Write only new data to the "file"
  .option("checkpointLocation", checkpointPathBronze)  # State is stored here.
  .start(outputPathBronze)                             # Start the job with the Delta path
)

# COMMAND ----------

print(cloud_storage_path)

# COMMAND ----------

# MAGIC %fs ls /Users/christopher.chalcraft@databricks.com/datagen

# COMMAND ----------

# MAGIC %fs ls /Users/christopher.chalcraft@databricks.com/datagen/bronze.delta/

# COMMAND ----------


