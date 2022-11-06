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

from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import string

def returnRandomJson():
  import datetime
  import random
  import json
  jsonObject={
    "createdAt": str(datetime.datetime.now()),
    "intRandom": random.randint(1, 9999),
    "stringRandom": ''.join(random.choice('abcdefghijklmnopqrtzusv') for i in range(10)),
    "eventType": random.choices(["event1", "event2"])[0]
  }
  return(json.dumps(jsonObject))

returnRandomJsonUDF = udf(returnRandomJson, StringType())

# COMMAND ----------

''.join(random.choice('abcdefghijklmnopqrtzusv') for i in range(10))

# COMMAND ----------


