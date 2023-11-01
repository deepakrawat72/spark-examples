package data_analysis

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

trait TestSparkSessionProvider {
  val spark: sql.SparkSession = SparkSession.builder().appName("test").master("local[*]")
    //.enableHiveSupport()
    .getOrCreate()

}
