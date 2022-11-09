# Databricks notebook source
from pyspark.sql.functions import from_json, col, schema_of_json, explode, get_json_object


# COMMAND ----------

sensor_path = "/Users/christopher.chalcraft@databricks.com/datagen/bronze.delta/"
cloudfile_settings = {
  "cloudFiles.useIncrementalListing": "true",
  "cloudFiles.rescuedDataColumn": "_rescued_data",
  "cloudFiles.inferColumnTypes": "true", #(need to specify schema in prod)
}

# COMMAND ----------


@dlt.create_table(
  comment="sensor data",
  table_properties={
    "quality": "bronze"
  }
)
@dlt.table
def sensor():
  return(
    (spark.readStream
    .format("cloudFiles")
    .option("cloudFile.format", "delta")
    .options(**cloudfile_settings)

    )
    
  )

# COMMAND ----------

df = (spark.readStream
.format("cloudFiles")
.option("cloudFiles.format")
)
