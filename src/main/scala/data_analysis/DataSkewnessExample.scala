package data_analysis

import org.apache.spark.sql.functions.{broadcast, col, concat, explode, lit, rand, randn, when}
import org.apache.spark.sql.types.{IntegerType, StringType}
import session.SparkSessionProvider

import scala.collection.immutable
import scala.util.Random

object DataSkewnessExample extends App with SparkSessionProvider {

  import spark.implicits._
  spark.conf.set("spark.sql.shuffle.partitions", 5)

  val list1: Seq[Int] = List.fill(10000000)(1)
    .union(List.fill(5000)(2))
    .union(List.fill(5000)(3))
    .union(List.fill(5000)(4))
    .union(List.fill(5000)(5))

  val list2: Seq[Int] = List.fill(500)(1)
    .union(List.fill(500)(2))
    .union(List.fill(500)(3))
    .union(List.fill(500)(4))
    .union(List.fill(500)(5))

  val data1 = spark.sparkContext.parallelize(list1, 5).toDF("id")
  val data2 = spark.sparkContext.parallelize(list2, 5).toDF("id")

  println("No of joined records :" + data1.join(data2, data1("id") === data2("id")).count())

  //handling skew
  val replicationFactor = 100
  //we will only do salting for skewed keys
  val salted_lhs = data1.withColumn("salted_key",
    when(col("id") === lit(1), concat(col("id"), lit("_"),
      (rand() * 10).cast(IntegerType) % replicationFactor))
      .otherwise(col("id"))
  )

  val createSaltedKeysList = (key: Int, rf: Int) =>
    if(key == 1) (0 until rf).map(i => s"${key}_$i")
    else Seq(key.toString)

  val saltedKeyListGenerator = spark.udf.register("createSaltedKeysList", createSaltedKeysList)

  //in right data for each id will create salted_key as 1_0, 1_1, 1_2, 1_3, 1_4, 500 * no_of_replication_keys
  val salted_rhs = data2.withColumn("salted_key", saltedKeyListGenerator(col("id"), lit(replicationFactor))
    .as("salted_key")
  ).select(col("id"), explode(col("salted_key")).as("salted_key"))

  val joinedData = salted_lhs.join(salted_rhs, salted_lhs("salted_key") === salted_rhs("salted_key"))

  println("No of joined records :" + joinedData.count())
//  print("no of partitions : " + joinedData.rdd.getNumPartitions)
//
//  joinedData.show(false)


  scala.io.StdIn.readLine()
}
