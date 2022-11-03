# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

import os
cwd = os.path.basename(os.getcwd())

# COMMAND ----------

# MAGIC %run ../_resources/global_setup $reset_all_data=$reset_all_data $db_prefix=datagen

# COMMAND ----------


