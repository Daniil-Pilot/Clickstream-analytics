from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, expr
from pyspark.sql.types import StructType, StringType, LongType

spark = SparkSession.builder.appName("stream_etl").getOrCreate()

# схема
schema = StructType() \
    .add("user_id", StringType()) \
    .add("session_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", StringType()) \
    .add("price", StringType()) \
    .add("timestamp", LongType()) \
    .add("page", StringType()) \
    .add("category", StringType())

# Читаем новые файлы из HDFS incoming directory
stream_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .load("hdfs://namenode:9000/data/raw/incoming/")

# преобразуем timestamp
stream_df = stream_df.withColumn("ts", (col("timestamp")/1000).cast("timestamp"))

# Вариант 1: если в данных есть session_id, сгруппировать по session_id
from pyspark.sql.functions import window
sessionized = stream_df.groupBy("user_id","session_id").agg(
    expr("min(ts) as session_start"),
    expr("max(ts) as session_end"),
    expr("count(*) as event_count"),
    expr("sum(case when event_type='purchase' then 1 else 0 end) as purchases")
)

# Записать в HDFS (append Parquet)
query = sessionized.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/streaming/sessions") \
    .option("checkpointLocation", "hdfs://namenode:9000/data/streaming/checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
