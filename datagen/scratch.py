# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

from pyspark.sql.functions import from_json, col, schema_of_json, explode, get_json_object

# COMMAND ----------

# MAGIC %fs ls /Users/christopher.chalcraft@databricks.com/datagen/bronze.delta/

# COMMAND ----------

# This is what we want

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

table_path = "/Users/christopher.chalcraft@databricks.com/datagen/bronze.delta/"
df = (spark.read
.format("delta")
.load(table_path)
)
display(df)

# COMMAND ----------



# COMMAND ----------

tmp_df= (spark.read
.format("delta")
.load(table_path)
.take(1)
)

json_string = tmp_df[0][1]
schema = schema_of_json(json_string)
print(schema)

# COMMAND ----------

tmp_df[0][1]

# COMMAND ----------

table_path = "/Users/christopher.chalcraft@databricks.com/datagen/bronze.delta/"
df = (spark.read
.format("delta")
.load(table_path)
.withColumn("payload",from_json("payload",schema))
.select("payload.*","*")
.drop("payload", "event")
)
display(df)

# COMMAND ----------

# Fix types - https://www.geeksforgeeks.org/how-to-change-column-type-in-pyspark-dataframe/
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DoubleType, LongType, ArrayType

coltype_map = {
  "GPSData": DecimalType(),
  "DataAlgorithmVersion": StringType(),
  "TripID": StringType(),
  "CompanyID": StringType(),
  "StartTimeZone": StringType(),
  "DriverID": StringType()
}

# COMMAND ----------



# COMMAND ----------


