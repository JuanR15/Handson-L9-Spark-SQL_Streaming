from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("RideSharingAnalytics_Task1").getOrCreate()
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

query = parsed_df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "outputs/task_1") \
    .option("checkpointLocation", "outputs/task_1_checkpoint") \
    .start()

query.awaitTermination()