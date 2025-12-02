from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.types import LongType

spark = SparkSession.builder.appName("batch_etl").getOrCreate()

# читаем все исторические файлы
raw = spark.read.json("hdfs://namenode:9000/data/raw/history/*.json")
# очистка
clean = raw.dropDuplicates().na.drop()

# ensure timestamp is timestamp
clean = clean.withColumn("ts", (col("timestamp")/1000).cast("timestamp"))

# Bronze
clean.write.mode("overwrite").partitionBy("category").parquet("hdfs://namenode:9000/data/bronze/clickstream")

# Sessionization using session_id if present, else create sessions by user/time window
# If session_id provided:
sessions = clean.groupBy("user_id","session_id").agg(
    expr("min(ts) as session_start"),
    expr("max(ts) as session_end"),
    expr("count(*) as event_count"),
    expr("sum(case when event_type='purchase' then 1 else 0 end) as purchases")
)

sessions.write.mode("overwrite").parquet("hdfs://namenode:9000/data/silver/sessions")

# Feature aggregation per session (gold)
from pyspark.sql.functions import (avg, min as _min, max as _max, countDistinct)
features = sessions.select("user_id","session_id","session_start","session_end","event_count","purchases")
features = features.withColumn("session_duration_seconds", expr("unix_timestamp(session_end)-unix_timestamp(session_start)"))
features.write.mode("overwrite").parquet("hdfs://namenode:9000/data/gold/session_features")
