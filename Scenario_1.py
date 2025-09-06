# Databricks notebook source
# MAGIC %md
# MAGIC ## **Quering source**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pyspark_cat.source.products

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col


# COMMAND ----------

# df = spark.read.table("pyspark_cat.source.products")
df = spark.sql("select * from pyspark_cat.source.products")

#Dedup
df = df.withColumn("dedup", row_number().over(Window.partitionBy('id').orderBy(desc('updated_date'))))

df = df.filter(col("dedup")==1).drop('dedup')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** UPSERTS **

# COMMAND ----------

# Create Delta Object

from delta.tables import DeltaTable

# IF data there then merge else create, either use if or try catch
# if dbutils.fs.ls("/Volumes/pyspark_cat/source/db_volume/product_sink/"):
if len(dbutils.fs.ls("/Volumes/pyspark_cat/source/db_volume/product_sink/")) > 0:
    dlt_obj = DeltaTable.forPath(spark, '/Volumes/pyspark_cat/source/db_volume/product_sink/')

    # dlt_obj.alias("trgt").merge

    dlt_obj.alias("tgt").merge(
        df.alias("src"),
        "src.id = tgt.id")\
            .whenMatchedUpdateAll(condition="src.updated_date >= tgt.updated_date")\
            .whenNotMatchedInsertAll()\
        .execute()
    print("This is upserting now")
else:
    df.write.format("delta")\
        .mode("Overwrite")\
        .save("/Volumes/pyspark_cat/source/db_volume/product_sink/")
    print("This is saving now")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/pyspark_cat/source/db_volume/product_sink/`