# Databricks notebook source
# MAGIC %md
# MAGIC ### import functions

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a widget for incremental run

# COMMAND ----------

dbutils.widgets.text("incremental flag", "0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check incremental run

# COMMAND ----------

incremental_flag = dbutils.widgets.get("incremental flag")
incremental_flag

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establish connection with ADLS

# COMMAND ----------

spark.conf.set("fs.azure.account.key.firstprojectstorageaccnt.dfs.core.windows.net",
               "i removed keys because of security we need to write server storage keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Connection

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@firstprojectstorageaccnt.dfs.core.windows.net/silverdata`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working on Product Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct(SalesRegion) as SalesRegion from parquet.`abfss://silver@firstprojectstorageaccnt.dfs.core.windows.net/silverdata`
# MAGIC
# MAGIC SELECT DISTINCT (SalesRegion)as SalesRegion
# MAGIC FROM parquet.`abfss://silver@firstprojectstorageaccnt.dfs.core.windows.net/silverdata`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## create a DataFrame - df_src

# COMMAND ----------

df_src = spark.sql("""
                   SELECT DISTINCT (SalesRegion)as SalesRegion FROM parquet.`abfss://silver@firstprojectstorageaccnt.dfs.core.windows.net/silverdata`
                   """)

df_src.display() 

# COMMAND ----------

df_src.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Gold Layer Data - df_sink

# COMMAND ----------

from delta.tables import DeltaTable

path = 'abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Region'

# if spark.catalog.tableExists("gold.dim_customer"):
if DeltaTable.isDeltaTable(spark, path):
  df_sink = spark.sql(
          """
               select Dim_Region_Key, SalesRegion from delta.`abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Region`
           """
        )
  print('Table exists')
# we can get with 2 types of schema from gold layer 1] with where condition 2] with create blank schema table
else:
# 1] in this we are getting schema from silver layer with 'where' condition to get blank schema table
  # df_sink = spark.sql(
  #         """
  #            select 1 as Dim_Region_Key, SalesRegion from parquet.`abfss://silver@firstprojectstorageaccnt.dfs.core.windows.net/silverdata` where 1 = 0
  #         """
  #       )
  df_sink = spark.createDataFrame([], schema = "Dim_Region_Key int, SalesRegion string")

df_sink.display()
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Old and new records - left join
# MAGIC

# COMMAND ----------

df = df_src.join(df_sink, df_src['SalesRegion']==df_sink['SalesRegion'],'left').select(df_src['SalesRegion'],df_sink['Dim_Region_Key'])
df.display()  


# COMMAND ----------

# MAGIC %md
# MAGIC ## Old Records

# COMMAND ----------

df_old = df.filter(df['Dim_Region_Key'].isNotNull())
df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## New Record

# COMMAND ----------

df_new = df.filter(df['Dim_Region_Key'].isNull())
df_new.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Maximum Dim_Customer_Key

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1

else:
    max_value = df_old.agg(max(df_old['Dim_Region_Key'])).collect()[0][0]
max_value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Surrogate Key

# COMMAND ----------

# DBTITLE 1,Cell 27
df_new = df_new.withColumn('Dim_Region_Key', max_value + monotonically_increasing_id())
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Old and New

# COMMAND ----------

# DBTITLE 1,Cell 29
df = df_old.union(df_new)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC scd means slowly changinf dimension
# MAGIC ## SCD Type 1 - Upsert

# COMMAND ----------

# DBTITLE 1,Untitled
# ACID Transactions - Atomicity Consistency Isolation Durability
from delta.tables import DeltaTable

table_name = 'gold.dim_Region'
path = 'abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Region'

# first we need to write this for the first time to create the table
#if spark.catalog.tableExists(table_name):  
if DeltaTable.isDeltaTable(spark, path):  # then we can merge the data into the table
  deltaTable = DeltaTable.forPath(spark, path)
  deltaTable.alias('target').merge(df.alias('source'),'target.Dim_Region_Key = source.Dim_Region_Key').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
  print('Table Now Exists')
else:
  df.write.format('delta').mode('overwrite').option('mergeSchema', "true").save(path)
  print('Table created')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Region`
