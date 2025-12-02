from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName("predict").getOrCreate()

model = LogisticRegressionModel.load("hdfs://namenode:9000/models/purchase_lr")

new_sessions = spark.read.parquet("hdfs://namenode:9000/data/streaming/sessions")
# prepare features: must match assembler used in training
assembler = VectorAssembler(inputCols=["event_count","session_duration_seconds"], outputCol="features")
df = assembler.transform(new_sessions).select("user_id","session_id","features")

pred = model.transform(df)
pred.select("user_id","session_id","probability","prediction").show(50)
