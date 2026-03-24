from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("RideSharingAnalytics_Task2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

raw_df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

parsed_df = raw_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

rides_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withWatermark("event_time", "1 minute")

agg_df = rides_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

def write_to_csv(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"outputs/task_2/batch_{batch_id}")

query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "outputs/task_2_checkpoint") \
    .start()

query.awaitTermination()