
   // create data frame for csv file
    val df = spark.read.csv("zipcode.csv")
        df.printSchema() // nullable columns


   //data frame with header
   val df1 = spark.read.option("header",true).csv("zipcode.csv")     
   df1.printSchema() // columns with header

   //multiple csv files from diff locations
    val df = spark.read.csv("path1,path2,path3")

    //Read all csvs from the folder path
    val df = spark.read.csv("Folder path")
 
    //infraschema , delimiter (,) and header
    val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("zipcode.csv")
    val df3 = spark.read.options(Map("delimiter"->",","header"->"true")).csv("zip.csv")

  //import org.apache.spark.sql.types.StructType
 // import org.apache.spark.sql.types.DoubleType  
  //import org.apache.spark.sql.types.BooleanType 

  import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField,DoubleType,BooleanType}

    // specify schema manually
    //If you know the schema of the file ahead and do not want to use the inferSchema option 
    //for column names and types, use user-defined custom column names and type using schema option

    val schema = new StructType().add("RecordNumber",IntegerType,true).add("Zipcode",IntegerType,true).add("ZipCodeType",StringType,true).add("City",StringType,true) 
        .add("State",StringType,true)
        .add("LocationType",StringType,true)
        .add("Lat",DoubleType,true)
        .add("Long",DoubleType,true)
        .add("Xaxis",IntegerType,true)
        .add("Yaxis",DoubleType,true)
        .add("Zaxis",DoubleType,true)
        .add("WorldRegion",StringType,true)
        .add("Country",StringType,true)
        .add("LocationText",StringType,true)
        .add("Location",StringType,true)
        .add("Decommisioned",BooleanType,true)
        .add("TaxReturnsFiled",StringType,true)
        .add("EstimatedPopulation",IntegerType,true)
        .add("TotalWages",IntegerType,true)
        .add("Notes",StringType,true)
    val df_with_schema = spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load("zipcode.csv")
     df_with_schema.printSchema()
     df_with_schema.show()

    // create a view
     df_with_schema.createOrReplaceTempView("zipdata")

     // run SQL queries
     val df2 = spark.sql("SELECT * from zipdata")

    // write the o/p to another file
     df2.write.mode("overwrite").csv("c:/sparkscala/tmp/zipcodes") 
     // or you can also write 
     df2.write.format("csv").mode("overwrite").save("c:/sparkscala/tmp/zipcodecsv")

     
  
//output
