// Databricks notebook source
def getSecret(secretKey: String) = {
  dbutils.secrets.get(scope = "sm13", key = secretKey)
}

spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net" , "OAuth");
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", getSecret("CLIENT-ID"));
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", getSecret("CLIENT-SECRET"));
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", getSecret("CLIENT-ENDPOINT"));
spark.conf.set("fs.azure.account.key.stm13westeurope.dfs.core.windows.net", getSecret("ACCOUNT-KEY-ST"));

val inputPath="abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather"
val outputPath="abfss://data@stm13westeurope.dfs.core.windows.net/data"

val bronzePath=s"${outputPath}/bronze"
val bronzeCheckpoint=s"${bronzePath}/checkpoint"

val silverPath=s"${outputPath}/silver"
val silverCheckpoint=s"${silverPath}/checkpoint"

val goldPath=s"${outputPath}/gold"
val goldCheckPoint=s"${goldPath}/checkpoint"

case class WeatherHotels(address: String,
                   avg_tmpr_c: Double,
                   avg_tmpr_f: Double,
                   city: String,
                   country: String,
                   geoHash: String,
                   id: String,
                   latitude: Double,
                   longitude: Double,
                   name: String,
                   wthr_date: String,
                   year: String,
                   month: String,
                   day: String)

// COMMAND ----------

dbutils.fs.rm(bronzePath, true)
dbutils.fs.rm(silverPath, true)

// COMMAND ----------

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

val weatherHotelsSchema = ScalaReflection.schemaFor[WeatherHotels].dataType.asInstanceOf[StructType]

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.partitionColumns", "year, month, day")
    .schema(weatherHotelsSchema)
    .load(inputPath)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronzeCheckpoint)
    .partitionBy("year", "month", "day")
    .queryName("Bronze")
    .start(bronzePath)

// COMMAND ----------
// Number of distinct hotels in the city.
val silver = spark
  .readStream
  .format("delta")
  .load(bronzePath)
  .withColumn("eventTime", $"wthr_date".cast("timestamp"))
  .withWatermark("eventTime", "1 day")
  .groupBy(window($"eventTime", "1 day"), $"wthr_date".cast("date").as("date"), $"city", $"year", $"month", $"day")
  .agg(
    approx_count_distinct("id").alias("distinct_hotels_counts_by_city"),
    max(col("avg_tmpr_c")).as("max_tempr"),
    min(col("avg_tmpr_c")).as("min_tempr"),
    round(avg(col("avg_tmpr_c")), 2).as("average_tempr"),
  )

silver.explain(true)

silver
  .writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", silverCheckpoint)
  .queryName("silver")
  .start(silverPath)

display(silver)

// COMMAND ----------

val gold = spark
  .read
  .format("delta")
  .load(silverPath)
  .groupBy("city")
  .agg( max("distinct_hotels_counts_by_city").alias("hotel_count"))
  .orderBy(desc("hotel_count"))
  .withColumn("rank", row_number().over(Window.orderBy(desc("hotel_count"))))
  .limit(10)

gold.explain(true);

display(gold);

// COMMAND ----------

val result = silver.join(gold, usingColumn = "city").orderBy(desc("hotel_count"))

result.explain(true)

display(result)

// Visualize 10 biggest cities (the biggest number of hotels in the city)
// COMMAND ----------

display(result.where($"rank" === 1))

// COMMAND ----------

display(result.where($"rank" === 2))

// COMMAND ----------

display(result.where($"rank" === 3))

// COMMAND ----------

display(result.where($"rank" === 4))

// COMMAND ----------

display(result.where($"rank" === 5))

// COMMAND ----------

display(result.where($"rank" === 6))

// COMMAND ----------

display(result.where($"rank" === 7))

// COMMAND ----------

display(result.where($"rank" === 8))

// COMMAND ----------

display(result.where($"rank" === 9))

// COMMAND ----------

display(result.where($"rank" === 10))

// COMMAND ----------


