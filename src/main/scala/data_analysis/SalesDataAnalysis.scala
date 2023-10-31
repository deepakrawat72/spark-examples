package data_analysis

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import session.SparkSessionProvider

object SalesDataAnalysis extends App with SparkSessionProvider {
  val productsData = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("C:\\Users\\Deepak Rawat\\Documents\\projects\\new\\spark-examples\\src\\main\\resources\\raw_files\\products.csv")

  val salesData = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("C:\\Users\\Deepak Rawat\\Documents\\projects\\new\\spark-examples\\src\\main\\resources\\raw_files\\sales.csv")

  val sellerData = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("C:\\Users\\Deepak Rawat\\Documents\\projects\\new\\spark-examples\\src\\main\\resources\\raw_files\\sellers.csv")

  //How many orders, products, sellers are there in the data
  //  println(salesData.count())
  //  println(productsData.count())
  //  println(sellerData.count())

  val countByProducts = salesData.groupBy("product_id").count().as("count")
  //.cache()
  //How many products have been sold at-least once?
  countByProducts.where(col("count") >= 1).select("product_id")
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
  val aggData = salesData.as("sales_data").join(sellerData.as("seller_data"), salesData("seller_id") === sellerData("seller_id"))
    .groupBy("product_id", "seller_data.seller_id").agg(sum("num_pieces_sold").as("total_order_sold"))
    .orderBy(col("product_id"), col("total_order_sold").desc)
    .withColumn("max_sales", max("total_order_sold").over(Window.partitionBy("product_id")))
    .withColumn("min_sales", min("total_order_sold").over(Window.partitionBy("product_id")))

  aggData.filter(col("total_order_sold") === col("max_order_sold"))
}
