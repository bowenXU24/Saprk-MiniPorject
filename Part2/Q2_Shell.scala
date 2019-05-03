import org.apache.spark.sql.types._
import org.apache.spark.sql.Row



val lines = sc.textFile("YOUR_SPARK_HOME/user_artists.dat")
val colNames = new Array[String](3)
colNames(0)="userID"

colNames(1)="artistID"

colNames(2)="weight"

val textrdd = lines.mapPartitionsWithIndex(
     (i,iterator)=>
     if(i==0 && iterator.hasNext){
     iterator.next
     iterator}
     else iterator)

val schema = StructType(colNames.map(fieldName => StructField(fieldName, IntegerType)))

val rowRDD = textrdd.map(_.split("\\s+").map(_.toInt)).map(p=>Row(p: _*))

val data = spark.createDataFrame(rowRDD,schema)

data.show(5)

val artistW = data.groupBy("artistID").agg(sum("weight"))

artistW.orderBy(desc("sum(weight)")).show

val order = artistW.orderBy(desc("sum(weight)"))

order.show()

//save output from to a text file
order.repartition(1).rdd.saveAsTextFile("YOUR_SPARK_HOME/output1")