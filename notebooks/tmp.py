import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
.builder \
.appName("mongodbtest1") \
.master('local')\
.config("spark.mongodb.input.uri", "mongodb://root:password@127.0.0.1:27017/news.dailytimes?authSource=admin") \
.config("spark.mongodb.output.uri", "mongodb://root:password@127.0.0.1:27017/news.dailytimes?authSource=admin") \
.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
.getOrCreate()

books_tbl = spark.read\
.format('com.mongodb.spark.sql.DefaultSource')\
.option( "uri", "mongodb://root:password@127.0.0.1:27017/news.dailytimes?authSource=admin") \
.load()

books_tbl.printSchema()

books_table = books_tbl.createOrReplaceTempView("books_tbl")

query1 = spark.sql("SELECT * FROM books_tbl ")
query1.show(5)