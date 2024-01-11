package session

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  val spark: sql.SparkSession = SparkSession.builder().appName("my_app").master("local[*]")
    .config("spark.driver.memory", "0.5g")
    //.enableHiveSupport()
    .getOrCreate()
}
