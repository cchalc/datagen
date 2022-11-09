# Databricks notebook source
# MAGIC %md
# MAGIC #### The purpose of this notebook is to generate messages that look like Even Hub messages which can be used to develop code against Event Hub, without using a real Event Hub

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define widgets

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text("workingDir", "/tmp/streaming_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set libraries and methods

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC import random
# MAGIC import string
# MAGIC 
# MAGIC def returnRandomJson():
# MAGIC   import datetime
# MAGIC   import random
# MAGIC   import json
# MAGIC   jsonObject={
# MAGIC     "createdAt": str(datetime.datetime.now()),
# MAGIC     "intRandom": random.randint(1, 9999),
# MAGIC     "stringRandom": ''.join(random.choice('abcdefghijklmnopqrtzusv') for i in range(10)),
# MAGIC     "eventType": random.choices(["event1", "event2"])[0]
# MAGIC   }
# MAGIC   return(json.dumps(jsonObject))
# MAGIC 
# MAGIC returnRandomJsonUDF = udf(returnRandomJson, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the schema for Event Hub messages

# COMMAND ----------

schemaEventHub = StructType([
  StructField("body", BinaryType(), True),              # Our JSON payload
  StructField("partition", StringType(), True),         # The partition from which this record is received
  StructField("offset", StringType(), True),            # The position of this record in the corresponding EventHubs partition
  StructField("sequenceNumber", LongType(), True),      # A unique identifier for a packet of data (alternative to a timestamp)
  StructField("enqueuedTime", TimestampType(), True),   # Time when data arrives
  StructField("publisher", StringType(), True),         # Who produced the message
  StructField("partitionKey", StringType(), True),      # A mechanism to access partition by key
  StructField("properties", MapType(StringType(), StringType()), True),           # Extra properties
  StructField("systemProperties", MapType(StringType(), StringType()), True)       # Extra properties
])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate StreamingDataFrame

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC streamingRateDF = (spark
# MAGIC   .readStream                            # Returns DataStreamReader
# MAGIC   .format("rate")                        # Rate source (for testing) - generates data at the specified number of rows per second
# MAGIC   .option("rowsPerSecond", 1)            # How many rows should be generated per second.
# MAGIC   .load()                                # schema is 'timestamp TIMESTAMP, value LONG'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add the Event Hub columns to the generated StreamingDataFrame

# COMMAND ----------

streamingEventHubDF = (streamingRateDF
  .withColumn("body", returnRandomJsonUDF().cast("binary"))
  .withColumn("partition", lit("0"))
  .withColumn("offset", col("value")+100)
  .withColumn("sequenceNumber", col("value"))
  .withColumn("enqueuedTime", col("timestamp"))
  .withColumn("publisher", lit("null"))
  .withColumn("partitionKey", lit("null"))
  .withColumn("properties", lit("{}"))
  .withColumn("systemProperties", lit("{}"))
  .drop("timestamp", "value")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start the stream with a memory sink for testing purposes
# MAGIC Stop the stream after a few seconds

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

# MAGIC %md
# MAGIC ### Query the memory sink to verity that the messages look like Event Hub messages

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM memorySinkStream

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the generated messages and transform as if they were Event Hub messages
# MAGIC `streamingEventHubDF` could easily be the DataFrame out of a real Event Hub

# COMMAND ----------

streamingProcessedDF = (streamingEventHubDF
  .select(
    year(col("enqueuedTime")).alias("year"),
    month(col("enqueuedTime")).alias("month"),
    get_json_object(col("body").cast("string"),"$.eventType").alias("event"),
    col("body").cast("string").alias("payload")
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the resulting messages to a Delta table

# COMMAND ----------

workingDir = dbutils.widgets.get("workingDir")
outputPathBronze =     workingDir + "/bronze.delta"
checkpointPathBronze = workingDir + "/bronze.checkpoint"


streamingQuery = (streamingProcessedDF                 # Start with the "streaming" DataFrame
  .writeStream                                         # Get the DataStreamWriter
  .queryName("deltaSinkStream")                        # Name the query
  .trigger(processingTime="5 seconds")                 # Configure for a 5-second micro-batch
  .format("delta")                                     # The output is stored in a delta table
  .partitionBy("event", "year", "month")
  .outputMode("append")                                # Write only new data to the "file"
  .option("checkpointLocation", checkpointPathBronze)  # State is stored here.
  .start(outputPathBronze)                             # Start the job with the Delta path
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Delta table

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(spark.sql("SELECT * FROM delta.`" + outputPathBronze + "` LIMIT 5"))

# COMMAND ----------


