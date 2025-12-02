from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.appName("train_model").getOrCreate()

data = spark.read.parquet("hdfs://namenode:9000/data/gold/session_features")
# Создадим label: purchase > 0 => 1 else 0
data = data.withColumn("label", when(col("purchases") > 0, 1).otherwise(0))

# Признаки — настраивай под себя
assembler = VectorAssembler(inputCols=["event_count","session_duration_seconds"], outputCol="features")
dataset = assembler.transform(data).select("features","label","session_id","user_id")

# train/test split
train, test = dataset.randomSplit([0.8,0.2], seed=42)

lr = LogisticRegression(maxIter=10, regParam=0.01)
model = lr.fit(train)

# Сохраняем модель
model.write().overwrite().save("hdfs://namenode:9000/models/purchase_lr")

# Оценка на тесте
pred = model.transform(test)
pred.select("session_id","probability","prediction").show(10)
