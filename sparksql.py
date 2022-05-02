C:/Users/Dummu/Desktop/Python/pandas-playbook-manipulating-data/data


df = spark.read.csv("C:/Users/Dummu/Desktop/Python/pandas-playbook-manipulating-data/data/weather.csv")


df2 = spark.read.option("header",True) \
     .csv("C:/Users/Dummu/Desktop/Python/pandas-playbook-manipulating-data/data/weather.csv")

from pyspark.sql.types import StructType,StructField, StringType , IntegerType



schema = StructType([ \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("time",IntegerType(),True), \
    StructField("temp", IntegerType(), True), \
    StructField("pressure", IntegerType(), True)
  ])



df4 = spark.read.options(inferSchema='True',delimiter=',') \
  .csv("src/main/resources/zipcodes.csv")


df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("C:/Users/Dummu/Desktop/Python/pandas-playbook-manipulating-data/data/weather.csv")
	  
	  df_with_schema.createOrReplaceTempView("wdata")
	  
	  df2 = spark.sql("SELECT * from wdata")
	  
	  
df2.write.mode('overwrite').csv("/tmp/spark_output/zipcodes")
//you can also use this
df2.write.format("csv").mode('overwrite').save("/tmp/spark_output/zipcodes")

df3 = spark.read.options(header='True', delimiter=',') \
  .csv("/tmp/resources/zipcodes.csv")
df3.printSchema()

reduce 

reduce_bye key

sort by key

C:\sparkscala\tmp\r
file:///C:/sparkscala/tmp/zipcodes/