# Databricks notebook source
import os
import platform
import pandas as pd
import pandas as to_csv
import pandas as DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import PCA
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import *

# COMMAND ----------

file_location = "/FileStore/tables/load.csv"
file_type = "csv"

infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

data = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(data)

# COMMAND ----------

data.select('id','name','email','phone','address','age','create_date','update_date').sort('id','update_date').show()

# COMMAND ----------

data1 = data[data.update_date >= '2018-04-21']

# COMMAND ----------

data2 = data1[data1.phone != '(11) 91234-5678']

# COMMAND ----------

data2.sort('id').show()

# COMMAND ----------

data2.toPandas()
