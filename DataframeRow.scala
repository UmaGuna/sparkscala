
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.{Row, SparkSession}

val simpleData = Seq(Row("James","","Smith","36636","M",3000),
    Row("Michael","Rose","","40288","M",4000),
    Row("Robert","","Williams","42114","M",4000),
    Row("Maria","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )

val simpleSchema = StructType(Array(
    StructField("firstname",StringType,true),
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))

val df = spark.createDataFrame(
     spark.sparkContext.parallelize(simpleData),simpleSchema)

     df.printSchema()
     df.show()


//output

scala> val df = spark.createDataFrame(
     |      spark.sparkContext.parallelize(simpleData),simpleSchema)
df: org.apache.spark.sql.DataFrame = [firstname: string, middlename: string ... 4 more fields]

scala> df.printSchema()
root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)


scala> df.show()
+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    James|          |   Smith|36636|     M|  3000|
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
|    Maria|      Anne|   Jones|39192|     F|  4000|
|      Jen|      Mary|   Brown|     |     F|    -1|
+---------+----------+--------+-----+------+------+

