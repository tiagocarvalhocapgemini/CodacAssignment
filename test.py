from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.read.format("csv").option("header","true").load("data/dataset_one.csv")
df.show(10)