# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup notbook used to initialize user and database

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("db_prefix", "datagen", "Database prefix")
dbutils.widgets.text("min_dbr_version", "9.1", "Min required DBR version")

# COMMAND ----------

from delta.tables import *
import pandas as pd
import logging
from pyspark.sql.functions import to_date, col, regexp_extract, rand, to_timestamp, initcap, sha1
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name
import re


# VERIFY DATABRICKS VERSION COMPATIBILITY ----------

try:
  min_required_version = dbutils.widgets.get("min_dbr_version")
except:
  min_required_version = "9.1"

version_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
version_search = re.search('^([0-9]*\.[0-9]*)', version_tag)
assert version_search, f"The Databricks version can't be extracted from {version_tag}, shouldn't happen, please correct the regex"
current_version = float(version_search.group(1))
assert float(current_version) >= float(min_required_version), f'The Databricks version of the cluster must be >= {min_required_version}. Current version detected: {current_version}'
assert "ml" in version_tag.lower(), f"The Databricks ML runtime must be used. Current version detected doesn't contain 'ml': {version_tag} "


# COMMAND ----------

def get_cloud_name():
  return spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider").lower()

# COMMAND ----------

# MAGIC %md
# MAGIC #### mount data

# COMMAND ----------

mount_name = "cchalc"

try:
  dbutils.fs.ls("/mnt/%s" % mount_name)
except:
  workspace_id = dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().get()
  url = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  dbutils.fs.mount(f"s3a://one-env-uc-external-location/shared_location/{mount_name}/" , f"/mnt/{mount_name}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Isolate user

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

db_prefix = dbutils.widgets.get("db_prefix")

dbName = db_prefix+"_"+current_user_no_at
cloud_storage_path = f"/Users/{current_user}/{db_prefix}"
reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)

spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
spark.sql(f"""USE {dbName}""")

print("using cloud_storage_path {}".format(cloud_storage_path))

# COMMAND ----------


