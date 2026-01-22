# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establish Connection

# COMMAND ----------

spark.conf.set("fs.azure.account.key.firstprojectstorageaccnt.dfs.core.windows.net",
                "i removed keys because of security we need to write server storage keys")

# COMMAND ----------

fileName = dbutils.fs.ls("abfss://bronze@firstprojectstorageaccnt.dfs.core.windows.net/")[0].name
fileName 

# COMMAND ----------

# we can read with two types of code 
# 1st type of code for reading file
# df = spark.read.parquet(f"abfss://bronze@firstprojectstorageaccnt.dfs.core.windows.net/{fileName}")

# 2nd type of code for reading file
df = spark.read.format("parquet").load(f"abfss://bronze@firstprojectstorageaccnt.dfs.core.windows.net/{fileName}")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning
# MAGIC

# COMMAND ----------

# it will convert all rows in capital of first letter
df = df.withColumn('Country', initcap(col('Country')))
df = df.withColumn('ProductName', initcap(col('ProductName')))
df = df.withColumn('ProductCategory', initcap(col('ProductCategory')))
df.display() 

# COMMAND ----------

df = df.dropna(how='any')
df.display()

# COMMAND ----------

df = df.dropDuplicates()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## putting data in silver

# COMMAND ----------

df.write.format("parquet").mode("overwrite").option("path","abfss://silver@firstprojectstorageaccnt.dfs.core.windows.net/silverdata").save()

# COMMAND ----------

