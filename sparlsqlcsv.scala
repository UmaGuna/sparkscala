 import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField,DoubleType,BooleanType}

 val schema = new StructType().add("RecordNumber",IntegerType,true).add("Zipcode",IntegerType,true).add("ZipCodeType",StringType,true).add("City",StringType,true).add("State",StringType,true)

 val df_with_schema = spark.read.format("csv").option("header", "true").schema(schema).load("Zipdata.csv")

     df_with_schema.printSchema()
     df_with_schema.show()

      df_with_schema.count

      //operations on data like count,avg,mean and can apply filters
     scala>  df_with_schema.count
     res25: Long = 5


// create a view
df_with_schema.createOrReplaceTempView("zipdata")

// run SQL queries
val df1 = spark.sql("SELECT * from zipdata")
df1.show()

+------------+-------+-----------+-------------+-----+
|RecordNumber|Zipcode|ZipCodeType|         City|State|
+------------+-------+-----------+-------------+-----+
|           1|  66223|   STANDARD|Overland park|   KS|
|           2|  66210|   STANDARD|Overland park|   KS|
|           3|  66213|   STANDARD|Overland park|   KS|
|           4|  66110|   STANDARD|       Shanee|   KS|
|           5|    660|     PO BOX|         XXXX|   KS|
+------------+-------+-----------+-------------+-----+

//total no of rows
val totCount = df1.count

scala>val totCount =  df1.count
res26: Long = 5

scala> val totCount = df1.count.toDouble
totCount: Double = 5.0

val OpCount = df1.filter($"City" === "Overland park").count.toDouble

scala> val OpCount = df1.filter($"City" === "Overland park").count.toDouble
OpCount: Double = 3.0

val perCount = OpCount/totCount * 100

scala> val perCount = (OpCount/totCount) * 100
perCount: Double = 60.0

df1.write.save("abczip") // create a folder and save the file in parquet format (default format)

//read the parquet file

scala> spark.read.load("abczip/part-00000-b4f5d9bd-4fea-4e0f-99b8-438befd688ab-c000.snappy.parquet").show()
+------------+-------+-----------+-------------+-----+
|RecordNumber|Zipcode|ZipCodeType|         City|State|
+------------+-------+-----------+-------------+-----+
|           1|  66223|   STANDARD|Overland park|   KS|
|           2|  66210|   STANDARD|Overland park|   KS|
|           3|  66213|   STANDARD|Overland park|   KS|
|           4|  66110|   STANDARD|       Shanee|   KS|
|           5|    660|     PO BOX|         XXXX|   KS|
+------------+-------+-----------+-------------+-----+


scala> df1.select(avg($"RecordNumber")).show()
+-----------------+
|avg(RecordNumber)|
+-----------------+
|              3.0|
+-----------------+


scala> df1.select(mean($"Zipcode")).show()
+------------+
|avg(Zipcode)|
+------------+
|     53083.2|
+------------+


//Select columns by regular expression
df1.select(df1.colRegex("`^.*City*`")).show()

scala> df1.select(df1.colRegex("`^.*City*`")).show()
+-------------+
|         City|
+-------------+
|Overland park|
|Overland park|
|Overland park|
|       Shanee|
|         XXXX|
+-------------+

//Fetch the columns starts with a word 'Zip' 

val dfZip = df1.select(df1.columns.filter(f=>f.startsWith("Zip")).map(m=>col(m)):_*)

scala> val dfZip = df1.select(df1.columns.filter(f=>f.startsWith("Zip")).map(m=>col(m)):_*)
dfZip: org.apache.spark.sql.DataFrame = [Zipcode: int, ZipCodeType: string]

scala> dfZip.show();
+-------+-----------+
|Zipcode|ZipCodeType|
+-------+-----------+
|  66223|   STANDARD|
|  66210|   STANDARD|
|  66213|   STANDARD|
|  66110|   STANDARD|
|    660|     PO BOX|
+-------+-----------+

//Fetch the columns ends with letter 'e'

val dfEndecol = df1.select(df1.columns.filter(f=>f.endsWith("e")).map(m=>col(m)):_*)

scala> val dfEndecol = df1.select(df1.columns.filter(f=>f.endsWith("e")).map(m=>col(m)):_*)
dfEndecol: org.apache.spark.sql.DataFrame = [Zipcode: int, ZipCodeType: string ... 1 more field]

scala> dfEndecol.show()
+-------+-----------+-----+
|Zipcode|ZipCodeType|State|
+-------+-----------+-----+
|  66223|   STANDARD|   KS|
|  66210|   STANDARD|   KS|
|  66213|   STANDARD|   KS|
|  66110|   STANDARD|   KS|
|    660|     PO BOX|   KS|
+-------+-----------+-----+


scala> dfEndecol.select($"Zipcode",$"State").show()
+-------+-----+
|Zipcode|State|
+-------+-----+
|  66223|   KS|
|  66210|   KS|
|  66213|   KS|
|  66110|   KS|
|    660|   KS|
+-------+-----+

scala> dfEndecol.filter($"Zipcode">66210).show()
+-------+-----------+-----+
|Zipcode|ZipCodeType|State|
+-------+-----------+-----+
|  66223|   STANDARD|   KS|
|  66213|   STANDARD|   KS|
+-------+-----------+-----+



// run SQL queries using where condition 

val df2 = spark.sql("SELECT * from zipdata where ZipCodeType='STANDARD'")

df2.show()

scala> df2.show()
+------------+-------+-----------+-------------+-----+
|RecordNumber|Zipcode|ZipCodeType|         City|State|
+------------+-------+-----------+-------------+-----+
|           1|  66223|   STANDARD|Overland park|   KS|
|           2|  66210|   STANDARD|Overland park|   KS|
|           3|  66213|   STANDARD|Overland park|   KS|
|           4|  66110|   STANDARD|       Shanee|   KS|
+------------+-------+-----------+-------------+-----+

df2.select("City").show(false)

scala> df2.select("City").show(false)
+-------------+
|City         |
+-------------+
|Overland park|
|Overland park|
|Overland park|
|Shanee       |
+-------------+


