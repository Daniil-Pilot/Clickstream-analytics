import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

st.title("Clickstream Analytics Dashboard")

spark = SparkSession.builder.appName("dashboard").getOrCreate()
# читаем агрегированные сессии (streaming output)
df = spark.read.parquet("hdfs://namenode:9000/data/streaming/sessions")
pandas_df = df.toPandas()

st.write("Последние сессии (sample):")
st.dataframe(pandas_df.head(100))

st.line_chart(pandas_df['event_count'].value_counts().sort_index())
