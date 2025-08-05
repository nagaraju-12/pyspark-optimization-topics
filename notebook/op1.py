# Databricks notebook source
# MAGIC %md
# MAGIC 1. Broadcast Join  
# MAGIC Goal: Avoid shuffle by broadcasting the small branch_lookup table

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("optimze").getOrCreate()

t_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/bank_transactions_sample.csv")
b_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/branch_lookup_sample.csv")

t_df.printSchema
b_df.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC 🔹 Q1.1: Join transactions with branch_lookup using BRANCH_CODE using broadcast().   
# MAGIC 🔹 Q1.2: Filter only CREDIT transactions after join and count them per REGION.

# COMMAND ----------

from pyspark.sql.functions import broadcast
join_tb=t_df.join(broadcast(b_df), on="BRANCH_CODE",how="inner")
join_tb.display()
#join_tb = t_df.join(broadcast(b_df), "BRANCH_CODE")
# COMMAND ----------

t_df.select("BRANCH_CODE").distinct().count()



# COMMAND ----------

b_df.select("BRANCH_CODE").distinct().count()

# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: Filter only CREDIT transactions
credit_txns = join_tb.filter(col("TXN_TYPE") == "CREDIT")

# Step 2: Group by REGION and count
credit_count_by_region = credit_txns.groupBy("REGION").count()

# Step 3: Display the result
credit_count_by_region.show()


# COMMAND ----------

join_tb.filter("TXN_TYPE = 'CREDIT'").groupBy("REGION").count().show()


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Shuffle Partition Tuning
# MAGIC Goal: Control the number of tasks during shuffle operations  
# MAGIC
# MAGIC 🔹 Q2.1: Before a groupBy, set spark.sql.shuffle.partitions = 10.   
# MAGIC 🔹 Q2.2: Group transactions by TXN_TYPE and calculate avg(AMOUNT) — observe stages.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",10)

# COMMAND ----------

from pyspark.sql.functions import avg
t_df.groupBy("TXN_TYPE").agg(avg("AMOUNT").alias("avg_amt")).display()
#t_df.groupBy("TXN_TYPE").agg(avg("AMOUNT").alias("avg_amt")).repartition(10).display()
print("output shuffle partitions",t_df.rdd.getNumPartitions())

# COMMAND ----------

print("output shuffle partitions",t_df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC Caching & Persisting  
# MAGIC Q3.1: Filter & cache EMI transactions > ₹20,000
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
tran_df=t_df.filter((col("TXN_TYPE")=="EMI")&(col("AMOUNT")>20000)).persist()
#tran_df=t_df.filter((col("TXN_TYPE")=="EMI")&(col("AMOUNT")>20000)).cache()
## Trigger an action to actually cache it in memory like :-.count(), .show(), or .collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Q3.2: Use cached result multiple times

# COMMAND ----------

tran_df.groupBy("CITY").count().show()

# COMMAND ----------


from pyspark.sql.functions import sum
tran_df.groupBy("CUSTOMER_ID").agg(sum("AMOUNT")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now open the Spark UI (if you're using Databricks, click the “View Spark UI” button):  
# MAGIC
# MAGIC ➡ Go to the "Storage" tab  
# MAGIC ➡ You’ll see an entry like:  
# MAGIC
# MAGIC RDD 123 [emi_df]  Storage Level: Memory/Deserialized  Cached Partitions: N  Size: X MB   
# MAGIC his confirms that .persist() actually happened.
# MAGIC
# MAGIC Without an action (like .count(), .show(), or .collect()), caching won’t be triggered.  
# MAGIC f you're using .cache() (which is .persist(MEMORY_AND_DISK) under the hood), you can check with: emi_df.is_cached   
# MAGIC Use storageLevel to confirm persist leve:-print(emi_df.storageLevel)
# MAGIC
# MAGIC

# COMMAND ----------

t_df.is_cached


# COMMAND ----------

print(t_df.storageLevel)

# COMMAND ----------

t_df.unpersist()

# COMMAND ----------

print(t_df.storageLevel)

# COMMAND ----------

# Example:here we can check upersist worked or not it will time
t_df.unpersist()

# Trigger an action to force recomputation (e.g., show timing)
import time

start = time.time()
t_df.count()
print("Time taken after unpersist:", time.time() - start)


# COMMAND ----------

from pyspark.sql.functions import col
import time

# Step 1: Filter and persist
tt_df = t_df.filter((col("TXN_TYPE") == "EMI") & (col("AMOUNT") > 20000)).persist()

# Step 2: Trigger action & time it
start = time.time()
t_df.count()
print("⏱ First count after persist:", time.time() - start)

# Step 3: Trigger again to see improvement
start = time.time()
t_df.count()
print("⏱ Second count (cached):", time.time() - start)

# Step 4: Unpersist and time again
t_df.unpersist()

start = time.time()
t_df.count()
print("⏱ After unpersist (recompute):", time.time() - start)


# COMMAND ----------

# MAGIC %md
# MAGIC 4. Coalesce vs Repartition   
# MAGIC 📌 What it does:   
# MAGIC Controls number of partitions for better performance or output file size.    
# MAGIC
# MAGIC ✅ Q4.1: Group and total per customer  
# MAGIC

# COMMAND ----------

agg_df=t_df.groupBy("CUSTOMER_ID").agg(sum("AMOUNT").alias("total")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC  Reduce to 2 output files

# COMMAND ----------

agg_df.coalesce(2).write.parquet("/FileStore/tables/customer_spend")

# COMMAND ----------

from pyspark.sql.functions import sum

agg_df = t_df.groupBy("CUSTOMER_ID").agg(sum("AMOUNT").alias("TOTAL_SPENT"))

# ✅ Now this will work
agg_df.coalesce(2).write.parquet("/FileStore/tables/customer_spend")


# COMMAND ----------

agg_df.show(5)
print(agg_df.storageLevel)  # Optional: if you're caching


# COMMAND ----------

# MAGIC %md
# MAGIC Based on the output you gave, here are the exact number and types of files created by Spark in the directory:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📁 **Directory: `/FileStore/tables/customer_spend`**
# MAGIC
# MAGIC | #   | File Name                        | Description                                     |
# MAGIC | --- | -------------------------------- | ----------------------------------------------- |
# MAGIC | 1️⃣ | `_SUCCESS`                       | ✅ **Indicates the job completed successfully.** |
# MAGIC | 2️⃣ | `_committed_5259078066361055282` | Internal file for tracking write commit.        |
# MAGIC | 3️⃣ | `_started_5259078066361055282`   | Internal file for tracking job start.           |
# MAGIC | 4️⃣ | `part-00000-...snappy.parquet`   | 💾 **Actual output data (only 1 part file).**   |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Summary:
# MAGIC
# MAGIC * **Total files**: **4**
# MAGIC * **Actual data files**: **1 Parquet file**
# MAGIC * **Supporting internal files**: **3 (\_SUCCESS, \_started, \_committed)**
# MAGIC
# MAGIC 🟡 Even though you used `.coalesce(2)`, only **one part file** (`part-00000`) was written. That means your dataset was small enough that Spark didn't need two files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want exactly 2 part files, use `repartition(2)` instead of `coalesce(2)`, like this:
# MAGIC
# MAGIC ```python
# MAGIC agg_df.repartition(2).write.parquet("/FileStore/tables/customer_spend")
# MAGIC ```
# MAGIC
# MAGIC Let me know if you'd like to force write 2 files and then check again.
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/customer_spend")


# COMMAND ----------

# MAGIC %md
# MAGIC 5. Delta Lake + Time Travel  
# MAGIC 📌 What it does:   
# MAGIC Delta lets you version tables and restore old versions.   
# MAGIC
# MAGIC ✅ Q5.1: Save EMI data to Delta  

# COMMAND ----------

tran_df.write.format("delta").save("/FileStore/delta/tran_data")


# COMMAND ----------

# MAGIC %md
# MAGIC Q5.2: Overwrite with CREDIT data

# COMMAND ----------

t_df.filter("TXN_TYPE='CREDIT'").write.format("delta").mode("overwrite").save("/FileStore/delta/tran_data")

# COMMAND ----------

# MAGIC %md
# MAGIC  Read previous version

# COMMAND ----------

df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/FileStore/delta/tran_data")
start = time.time()
print("show time", time.time() - start)


# COMMAND ----------

import time
start = time.time()
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/FileStore/delta/tran_data")
df_v0.show()
end = time.time()

print("Read + Show time:", end - start, "seconds")


# COMMAND ----------

# MAGIC %md
# MAGIC 6. Adaptive Query Execution (AQE)  
# MAGIC 📌 What it does:     
# MAGIC Automatically improves joins, skewed data, partition sizes at runtime.  
# MAGIC
# MAGIC ✅ Q6.1: Enable AQE

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)



# COMMAND ----------

# MAGIC %md
# MAGIC Q6.2: Run join and see adaptive plan

# COMMAND ----------

join_tb.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC 7. Full Optimized ETL Pipeline  
# MAGIC Combine broadcast, persist, shuffle tuning, write

# COMMAND ----------

# Broadcast join
df = t_df.join(broadcast(b_df), "BRANCH_CODE")

# Filter & persist
df_high = df.filter((col("TXN_TYPE") == "DEBIT") & (col("AMOUNT") > 15000)).persist()

# Group by REGION
df_high.groupBy("REGION").agg({"AMOUNT": "sum"}).show()

# Save
df_high.write.format("delta").mode("overwrite").save("/FileStore/delta/high_debit")

# Unpersist
df_high.unpersist()


# COMMAND ----------

# MAGIC %md
# MAGIC 8. Partitioning   
# MAGIC 📌 What it does:   
# MAGIC Splits saved data into folders based on column values → fast filter.  
# MAGIC
# MAGIC ✅ Q8.1: Save partitioned by CITY and TXN_TYPE  

# COMMAND ----------

t_df.write.partitionBy("CITY", "TXN_TYPE").mode("overwrite").parquet("/FileStore/tables/partitioned_txns")

# COMMAND ----------

# MAGIC %md
# MAGIC  Q8.2: Read only 'Delhi' DEBIT txns

# COMMAND ----------

spark.read.parquet("/FileStore/tables/partitioned_txns").filter("CITY='Delhi' AND  TXN_TYPE='DEBIT'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Bucketing 
# MAGIC 📌 What it does: 
# MAGIC Buckets (splits) data by hash on a column, helps in join optimization. 
# MAGIC
# MAGIC ✅ Q9.1: Save as bucketed table
# MAGIC
# MAGIC

# COMMAND ----------

t_df.write.bucketBy(8, "CUSTOMER_ID").sortBy("CUSTOMER_ID") \
    .mode("overwrite") \
    .saveAsTable("bucketed_transactions")


# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Q9.2: Join with another bucketed table

# COMMAND ----------

df1 = spark.table("bucketed_transactions")
df2 = spark.table("bucketed_customers")  # also bucketed by CUSTOMER_ID

df1.join(df2, "CUSTOMER_ID").show()
