
package com.sparkbyexamples.spark.dataframe //1: error: illegal start of definition
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.{Row, SparkSession}

import spark.implicits._

val structureData = Seq(
    Row(Row("James","","Smith"),"36636","M",3100),
    Row(Row("Michael","Rose",""),"40288","M",4300),
    Row(Row("Robert","","Williams"),"42114","M",1400),
    Row(Row("Maria","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
)

val structureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)



val df2 = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),structureSchema)
df2.printSchema()
df2.show()


//output


scala> val df2 = spark.createDataFrame(
     |     spark.sparkContext.parallelize(structureData),structureSchema)
df2: org.apache.spark.sql.DataFrame = []

scala> df2.printSchema()
root


scala> df2.show()
++
||
++
||
||
||
||
||
++