
val spark:SparkSession = SparkSession.builder()
   .master("local[1]").appName("SparkByExamples.com")
   .getOrCreate()

import spark.implicits._
val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))


val rdd = spark.sparkContext.parallelize(data)

// Create DF using toDF() method 
val dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()



val dfFromRDD1 = rdd.toDF("language","users_count")
dfFromRDD1.printSchema()

// Create DF using createDataFrame() method

val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
dfFromRDD2.printSchema()

//Using createDataFrame() with the Row type


import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
val schema = StructType( Array(
                 StructField("language", StringType,true),
                 StructField("language", StringType,true)
             ))
val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)


//Using toDF() on List or Seq collection

import spark.implicits._
val dfFromData1 = data.toDF() 

//Using createDataFrame() from SparkSession

//From Data (USING createDataFrame)
var dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)


https://sparkbyexamples.com/spark/different-ways-to-create-a-spark-dataframe/