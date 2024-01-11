package lakehouse

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit}
import session.SparkSessionProvider

object BronzeLayer extends App with SparkSessionProvider {

  import spark.implicits._

  private val productsRawPath = "./src/main/resources/raw_files/products.csv"
  private val productsBronzePath = "./src/main/resources/lakehouse/bronze/products"

  val productsData = spark.read.option("header", "true").option("inferSchema", "true")
    .csv(productsRawPath)

  productsData.write.format("delta").save(productsBronzePath)

  val productsDeltaTable = DeltaTable.forPath(spark, productsBronzePath)

  showData()

  showHistory()

  //delete record from delta table
  productsDeltaTable.delete(col("product_id") === lit(5))

  showData()

  showHistory()

  val newDF = spark.sparkContext.parallelize(Seq((14,"product_14",105))).toDF("product_id", "product_name", "price")

  //inserts records from delta table
  newDF.write.format("delta").mode("append").save(productsBronzePath)

  showData()

  showHistory()

  showData(0)

  productsDeltaTable.vacuum().executeCompaction()


  def showData(version: Int = -1): Unit = {
    if(version == -1)
      spark.read.format("delta").load(productsBronzePath).show(false)
    else
      spark.read.format("delta").option("versionAsOf", version).load(productsBronzePath).show(false)
  }

  def showHistory(): Unit = {
    productsDeltaTable.history().show(1000, truncate = false)
  }
}
