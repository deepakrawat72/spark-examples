package data_analysis

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import session.SparkSessionProvider

object SalesDataAnalysis extends App with SparkSessionProvider {
  val productsData = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("./src/main/resources/raw_files/products.csv")

  val salesData = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("./src/main/resources/raw_files/sales.csv")

  val sellerData = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("./src/main/resources/raw_files/sellers.csv")

  //How many orders, products, sellers are there in the data
  println(salesData.count())
  println(productsData.count())
  println(sellerData.count())


  //How many products have been sold at-least once?
  val productsSoldAtleastOnce = getProductsSoldAtleastOnce(salesData)
  //.show(false)

  //Which product is sold maximum
  //println(countByProducts.orderBy(desc("count")).first())

  //How many distinct products have been sold in each day?
  salesData.select("date", "product_id").distinct().groupBy("date")
    .agg(count("product_id").as("no_of_products"))
  //.show()

  //What is the average revenue of the orders?
  salesData.as("sales_data").join(productsData.as("product_date"), salesData("product_id") === productsData("product_id"))
    .withColumn("total_sales", col("num_pieces_sold") * col("price"))
    .select("total_sales")
    .agg(round(avg("total_sales"), 3).as("average_orders_revenue"))
  //    .show(false)

  //for each seller, what is the average % contribution of an order to the sellers’ daily quota?
  salesData.join(sellerData, salesData("seller_id") === sellerData("seller_id"))
    .withColumn("ratio", salesData("num_pieces_sold") / sellerData("daily_target"))
    .groupBy(salesData("seller_id")).agg(avg("ratio").as("contribution %"))
  //  .show(false)


  //Who are the second most selling and the least selling persons (sellers) for each product? Who are those for the product with product_id = 0

  val allSellers = getSecondBestAndWorstSellers(salesData, sellerData)
  //second top seller and least seller of product 0
  allSellers.filter("product_id == 0").show()


  def getSecondBestAndWorstSellers(salesData: Dataset[Row], sellerData: Dataset[Row]): Dataset[Row] = {
    val aggData = salesData.as("sales_data")
      .join(sellerData.as("seller_data"), salesData("seller_id") === sellerData("seller_id"))
      .groupBy("product_id", "seller_data.seller_id")
      .agg(sum("num_pieces_sold").as("total_order_sold"))
      .withColumn("rank_desc",
        dense_rank().over(Window.partitionBy("product_id").orderBy(desc("total_order_sold"))))
      .withColumn("rank_asc",
        dense_rank().over(Window.partitionBy("product_id").orderBy(asc("total_order_sold"))))

    //get sellers for which max orders == min orders
    val singleSeller = aggData.where(col("rank_desc") === col("rank_asc"))
      .select(col("product_id").as("single_seller_product_id"), col("seller_id").as("single_seller_id"),
        lit("Only seller or multiple sellers with the same results").as("type"))

    //get sellers who has second most orders
    val secondSeller = aggData.where(col("rank_desc") === 2)
      .select(col("product_id").as("second_seller_product_id"), col("seller_id").as("second_seller_id"),
        lit("Second top seller").as("type"))

    //get sellers who has least orders. Exclude single sellers and second best sellers
    val least_seller = aggData.where(col("rank_asc") === 1)
      .select(col("product_id"), col("seller_id"),
        lit("Least seller").as("type"))
      .join(secondSeller, col("second_seller_id") === col("seller_id")
        && col("second_seller_product_id") === col("product_id"), "left_anti")
      .join(singleSeller, col("single_seller_id") === col("seller_id")
        && col("single_seller_product_id") === col("product_id"), "left_anti")

    singleSeller.select(col("single_seller_product_id").as("product_id"),
      col("single_seller_id").as("seller_id"), col("type"))
      .union(secondSeller.select(
        col("second_seller_product_id").as("product_id"),
        col("second_seller_id").as("seller_id"),
        col("type"))
      ).union(least_seller)
  }

  private def getProductsSoldAtleastOnce(salesData: Dataset[Row]): Dataset[Row] = {
    salesData.groupBy("product_id").count().as("count")
      .where(col("count") >= 1).select("product_id")
  }

  def runningAvgOfSalesEvery3Months(salesData: Dataset[Row]): DataFrame = {
    val runningAvgWindow = Window.partitionBy("product_id").orderBy("month")
      .rowsBetween(-2, Window.currentRow)

    salesData.groupBy(col("product_id"), month(col("date")).as("month"))
      .agg(sum(col("num_pieces_sold")).as("total_pieces_sold"))
      .withColumn("running avg every 3 months", avg("total_pieces_sold")
        .over(runningAvgWindow))
  }

  def totalSalesByQuarterly(salesData: Dataset[Row]): DataFrame = {
    val quarterlySalesData =
      salesData.select(col("date"), col("num_pieces_sold"))
        .withColumn("year", year(col("date")))
        .withColumn("quarter",
          when(month(col("date")) >= 1 && month(col("date")) <= 3, "1")
            .when(month(col("date")) >= 4 && month(col("date")) <= 6, "2")
            .when(month(col("date")) >= 7 && month(col("date")) <= 9, "3")
            .when(month(col("date")) >= 10 && month(col("date")) <= 12, "4")
        ).drop("date")
        .groupBy("year")

    quarterlySalesData
      .agg(
        sum(when(col("quarter") === 1, col("num_pieces_sold")).otherwise(lit(0))).as("1st Quarter Sales"),
        sum(when(col("quarter") === 2, col("num_pieces_sold")).otherwise(lit(0))).as("2nd Quarter Sales"),
        sum(when(col("quarter") === 3, col("num_pieces_sold")).otherwise(lit(0))).as("3rd Quarter Sales"),
        sum(when(col("quarter") === 4, col("num_pieces_sold")).otherwise(lit(0))).as("4th Quarter Sales")
      )

    //using pivot
    quarterlySalesData.pivot("quarter").agg(coalesce(sum("num_pieces_sold"), lit(0)))
      .toDF("year", "1st Quarter Sales", "2nd Quarter Sales", "3rd Quarter Sales", "4th Quarter Sales")
  }
}
