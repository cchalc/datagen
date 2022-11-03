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

import dbldatagen as dg

# various parameter values
row_count = 1000
time_to_run = 15
rows_per_second = 5

# COMMAND ----------

time_now = int(round(time.time() * 1000))
base_dir = f"{data_path}/datagenerator_{time_now}"
test_dir = os.path.join(base_dir, "data")
checkpoint_dir = os.path.join(base_dir, "checkpoint")

# COMMAND ----------

schema = StructType([
  StructField("GPSData", ArrayType(StructType[
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
  ]), True),
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


