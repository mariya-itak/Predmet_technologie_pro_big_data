from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://5fd9f7e6d87d:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

spark.stop()

