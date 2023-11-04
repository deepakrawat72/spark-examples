package data_analysis

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import session.SparkSessionProvider

object ReadFixedWithFileWithHdrFtr extends SparkSessionProvider with App {

  if(args.length < 2) {
    throw new IllegalArgumentException("Invalid no. of args provided. Expected a) input file b) output location")
  }

  val rawData: DataFrame = spark.read.text(args(0))

  val headerRecord = rawData.where(col("value").startsWith("HEADER"))
  val footerRecord = rawData.where(col("value").startsWith("FOOTER"))
  val dataRecords = rawData.where(!(col("value").startsWith("HEADER")
    || col("value").startsWith("FOOTER")))

  val headerDate = headerRecord.collect().head.getAs[String]("value").split("\\|")(1)
  val footerCount = footerRecord.collect().head.getAs[String]("value").split("\\|")(1).toLong

  val schema = StructType(Array(
    StructField("col1", StringType),
    StructField("col2", StringType),
    StructField("col3", StringType),
    StructField("col4", StringType),
    StructField("col5", StringType),
    StructField("date", StringType)
  ))

  if (dataRecords.count() != footerCount) {
    throw new RuntimeException("File counts did not matched")
  }

  val data = dataRecords.map(row => createDataRecords(row))(RowEncoder(schema))
    .withColumn("date", to_date(col("date"), "yyyyMMdd"))

  val createDataRecords = (row: Row) => {
    val value = row.getAs[String]("value")
    val tokens = value.split("\\|")
    Row(tokens(0), tokens(1), tokens(2), tokens(3), tokens(4), headerDate)
  }

  data.write.partitionBy("date").json(args(1))
}
