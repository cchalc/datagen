# Databricks notebook source
pip install Faker

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

from pyspark.sql.functions import explode, sequence, to_date

beginDate = '2022-10-01'
endDate = '2022-10-20'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

# MAGIC %sql select * from dates

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dim_calendar as
# MAGIC select
# MAGIC   year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
# MAGIC   calendarDate,
# MAGIC   year(calendarDate) AS calendarYear,
# MAGIC   date_format(calendarDate, 'MMMM') as calendarMonth,
# MAGIC   month(calendarDate) as MonthOfYear,
# MAGIC   date_format(calendarDate, 'EEEE') as calendarDay,
# MAGIC   dayofweek(calendarDate) AS dayOfWeek,
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(calendarDate) as dayOfMonth,
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as isLastDayOfMonth,
# MAGIC   dayofyear(calendarDate) as dayOfYear,
# MAGIC   weekofyear(calendarDate) as weekOfYearIso,
# MAGIC   quarter(calendarDate) as quarterOfYear,
# MAGIC   to_date(dateadd(day, 7 - dayofweek(calendarDate), calendarDate)) as lastCompleteWeek,
# MAGIC   last_day(calendarDate) as lastCompleteMonth
# MAGIC   
# MAGIC  
# MAGIC from
# MAGIC   dates
# MAGIC order by
# MAGIC   calendarDate

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_calendar;

# COMMAND ----------

from faker import Faker
from datetime import date, timedelta
import random
import pandas as pd

fake = Faker()

# COMMAND ----------


