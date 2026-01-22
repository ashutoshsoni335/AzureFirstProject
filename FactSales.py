# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establish Connection

# COMMAND ----------

spark.conf.set("fs.azure.account.key.firstprojectstorageaccnt.dfs.core.windows.net",
               "i removed keys because of security we need to write server storage keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load All Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Dimension

# COMMAND ----------

df_customer = spark.sql(
              """
                select * from delta.`abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Customer`
              """
              )

df_customer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Dimension

# COMMAND ----------

df_product = spark.sql(
              """
                select * from delta.`abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Product`
              """
              )
df_product.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Dimension

# COMMAND ----------

df_date = spark.sql(
              """
                select * from delta.`abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Date`
              """
              )
df_date.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Region Dimension

# COMMAND ----------

df_salesRegion = spark.sql(
              """
                select * from delta.`abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_Region`
              """
              )
df_salesRegion.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

silver_df = spark.sql(
              """
                select * from parquet.`abfss://silver@firstprojectstorageaccnt.dfs.core.windows.net/silverdata`
                """
              )
silver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fact Table
# MAGIC

# COMMAND ----------

fact_df = silver_df.join(df_customer, silver_df['CustomerID']==df_customer['CustomerID'], 'left').join(df_product, silver_df['ProductID']==df_product['ProductID'], 'left').join(df_date, silver_df['OrderDate']==df_date['OrderDate'], 'left').join(df_salesRegion, silver_df['SalesRegion']==df_salesRegion['SalesRegion'], 'left').select(df_customer['Dim_Customer_Key'], df_product['Dim_Product_Key'], df_date['Dim_Date_Key'], df_salesRegion['Dim_Region_Key'],silver_df['UnitPrice'],silver_df['TotalPrice'],silver_df['Quantity'])
     
fact_df.display() 

# COMMAND ----------

# DBTITLE 1,Cell 17
from delta.tables import DeltaTable

table_name = 'gold.dim_factSales'  # this is a variable that only stores the name of the table 
path = 'abfss://gold@firstprojectstorageaccnt.dfs.core.windows.net/dim_factSales'
 
if DeltaTable.isDeltaTable(spark, path):  
  deltaTable = DeltaTable.forPath(spark, path)
  deltaTable.alias('target').merge(fact_df.alias('source'),'target.Dim_Customer_Key==source.Dim_Customer_Key and target.Dim_Product_Key==source.Dim_Product_Key and target.Dim_Date_Key==source.Dim_Date_Key and target.Dim_Region_Key==source.Dim_Region_Key').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
  print('Table Now Exists')
else:
  fact_df.write.format('delta').mode('overwrite').option('mergeSchema', "true").save(path)
  print('Table created')
