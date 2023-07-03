from doctest import DocFileTest
import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline


spark = SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[4]")\
    .config("spark.driver.memory","4G")\
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.1.0")\
    .config("spark.mongodb.input.uri", "mongodb://root:password@127.0.0.1:27017/news.dailytimes?authSource=admin") \
    .config("spark.mongodb.output.uri", "mongodb://root:password@127.0.0.1:27017/news.dailytimes?authSource=admin") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

df = spark.read\
.format('com.mongodb.spark.sql.DefaultSource')\
.option( "uri", "mongodb://root:password@127.0.0.1:27017/news.dailytimes_new?authSource=admin") \
.load()

# df.printSchema()
df.show(3, False)

# df.write.parquet("./tmp/news.parquet")
