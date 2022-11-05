# Databricks notebook source
# MAGIC %md 
# MAGIC ### configure

# COMMAND ----------

pip install dbldatagen

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %fs ls /mnt/cchalc/datagen

# COMMAND ----------

data_path = "/mnt/cchalc/datagen"

# COMMAND ----------

print(cloud_storage_path)

# COMMAND ----------

# MAGIC %fs ls /Users/christopher.chalcraft@databricks.com/datagen

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

print(schema_sensor)

# COMMAND ----------

# Create empty sensor data to check schema
empty_RDD = spark.sparkContext.emptyRDD()
sensor_df = spark.createDataFrame(
  data=empty_RDD,
  schema = schema_sensor,
  )

# COMMAND ----------

sensor_df.printSchema()

# COMMAND ----------

import dbldatagen as dg

# # various parameter values
# row_count = 1000
# time_to_run = 15
# rows_per_second = 5

# time_now = int(round(time.time() * 1000))
# base_dir = f"{data_path}/datagenerator_{time_now}"
# test_dir = os.path.join(base_dir, "data")
# checkpoint_dir = os.path.join(base_dir, "checkpoint")

# COMMAND ----------

(
  sensor_df.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("raw_sensor")
  )

# COMMAND ----------

schema = spark.table("raw_sensor").schema
print(schema)

# COMMAND ----------

# define dataspec
shuffle_partitions_requested = 4
partitions_requested = 4
data_rows = 100
# schema ==> defined above from spark table

dataspec = dg.DataGenerator(
    spark,
    name="test_data",
    rows=data_rows,
    partitions=shuffle_partitions_requested,
    randomSeedMethod="hash_fieldname",
    #verbose=True,
).withSchema(schema)

# COMMAND ----------

dataspec = (dataspec
.withColumnSpec("")
)

# COMMAND ----------

dbutils.fs.ls(f"{cloud_storage_path}")

# COMMAND ----------

df_with_schema = (
  spark.read
  .schema(schema)
  .json("/Users/christopher.chalcraft@databricks.com/datagen/tmp.json")
)
# df_with_schema.printSchema()
# df_with_schema.show()

# COMMAND ----------

print(cloud_storage_path)

# COMMAND ----------


