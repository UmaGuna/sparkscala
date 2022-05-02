## Scala Spark Commands

* Task1


# create RDD for arrays through parallelizm

val num = Array(1,2,3,4,5,6)
val NewData = sc.parallelize(num)


# spark actions

NewData.count()    // 6

NewData.take(3)    // Array(1,2,3)

NewData.foreach(println) // print the rdd array 


val rddCollect = NewData.collect()
println("Number of Partitions: "+NewData.getNumPartitions)
println("Action: First element: "+NewData.first())
println("Action: RDD converted to Array[Int] : ") rddCollect.foreach(println)



Spark Application UI: http://localhost:4040/
Resource Manager: http://localhost:9870
Spark JobTracker: http://localhost:8088/
Node Specific Info: http://localhost:8042/


* Teask 2

contents for data.txt

Welcome to spark scala
----------
# create RDD for the data.txt file

Val data = sc.textFile("C:\\sparkscala/data.txt")

# count for RDD

data.count()  // 1

data.take(1)  // Welcome to spark scala


* Task3

## Following are the three commands that we shall use for Word Count Example in Spark Shell :

# /** map */
var map = sc.textFile("C:\\sparkscala/data.txt").flatMap(line => line.split(" ")).map(word => (word,1));
 
# /** reduce */
var counts = map.reduceByKey(_ + _);
 
# /** save the output to file */
counts.saveAsTextFile("C:\\sparkscala/output/")





