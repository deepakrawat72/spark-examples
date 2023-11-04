package data_analysis

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class SalesDataAnalysisTest extends FeatureSpec with GivenWhenThen with Matchers with TestSparkSessionProvider {

  feature("Sales data analysis") {
    scenario("Find second best and wort sellers per product") {
      Given("Sales data and Sellers data")
      When("getSecondBestAndWorstSellers is invoked")
      import spark.implicits._
      val salesData = spark.sparkContext.parallelize(List(
        (1, 1, 1, "2020-07-07", 100, "bill1"),
        (1, 1, 1, "2020-07-07", 50, "bill2"),
        (1, 1, 2, "2020-07-07", 25, "bill3"),
        (1, 2, 1, "2020-07-07", 27, "bill4"),
        (1, 3, 1, "2020-07-07", 27, "bill5"),
        (1, 3, 2, "2020-07-07", 29, "bill6"),
        (1, 3, 3, "2020-07-07", 11, "bill7")
      )).toDF("order_id", "product_id", "seller_id", "date", "num_pieces_sold", "bill_raw_text")

      val sellerData = spark.sparkContext.parallelize(List(
        (1, "seller1", 100),
        (2, "seller2", 200),
        (3, "seller3", 300)
      )).toDF("seller_id", "seller_name", "daily_target")

      val secondBestAndWorstSellers = SalesDataAnalysis.getSecondBestAndWorstSellers(salesData, sellerData)

      val actualResult = secondBestAndWorstSellers.orderBy("product_id", "seller_id").collect()
      val expectedResult = Array(
        Row(1, 2, "Second top seller"),
        Row(2, 1, "Only seller or multiple sellers with the same results"),
        Row(3, 1, "Only seller or multiple sellers with the same results"),
        Row(3, 1, "Second top seller"),
        Row(3, 3, "Least seller")
      )

      assert(actualResult sameElements expectedResult)
    }

    scenario("Find running average of last 3 months") {
      Given("Sales data")
      When("runningAvgOfSalesEvery3Months is invoked")
      import spark.implicits._
      val salesData = spark.sparkContext.parallelize(List(
        (1, 1, 1, "2020-07-07", 100, "bill1"),
        (1, 1, 1, "2020-07-07", 50, "bill2"),
        (1, 1, 2, "2020-08-07", 25, "bill3"),
        (1, 2, 1, "2020-09-07", 27, "bill4"),
        (1, 3, 1, "2020-10-07", 27, "bill5"),
        (1, 3, 2, "2020-10-07", 29, "bill6"),
        (1, 3, 3, "2020-11-07", 11, "bill7"),
        (1, 3, 2, "2020-12-07", 29, "bill6"),
        (1, 3, 3, "2020-06-07", 11, "bill7"),
        (1, 3, 2, "2020-05-07", 29, "bill6"),
        (1, 3, 3, "2020-01-07", 11, "bill7"),
        (1, 3, 3, "2020-03-07", 11, "bill7")
      )).toDF("order_id", "product_id", "seller_id", "date", "num_pieces_sold", "bill_raw_text")

      salesData.withColumn("cumulativeSum", sum("num_pieces_sold")
        .over(Window.partitionBy("product_id").orderBy(month(col("date")))))
        .show(false)

      val runningAvg = SalesDataAnalysis.runningAvgOfSalesEvery3Months(salesData)
//      runningAvg.show(false)
    }

    scenario("Find total sales by each quarter") {
      Given("Sales data")
      When("totalSalesByQuarterly is invoked")
      import spark.implicits._
      val salesData = spark.sparkContext.parallelize(List(
        (1, 1, 1, "2020-07-07", 100, "bill1"),
        (1, 1, 1, "2020-07-07", 50, "bill2"),
        (1, 1, 2, "2020-08-07", 25, "bill3"),
        (1, 2, 1, "2020-09-07", 27, "bill4"),
        (1, 3, 1, "2020-10-07", 27, "bill5"),
        (1, 3, 2, "2020-10-07", 29, "bill6"),
        (1, 3, 3, "2020-11-07", 11, "bill7"),
        (1, 3, 2, "2020-12-07", 29, "bill6"),
        (1, 3, 3, "2020-06-07", 11, "bill7"),
        (1, 3, 2, "2020-05-07", 29, "bill6"),
        (1, 3, 3, "2020-01-07", 11, "bill7"),
        (1, 3, 3, "2021-03-07", 11, "bill7"),
        (1, 3, 2, "2021-01-07", 29, "bill6"),
        (1, 3, 3, "2021-02-07", 11, "bill7"),
        (1, 3, 2, "2021-05-07", 29, "bill6"),
        (1, 3, 3, "2021-05-07", 11, "bill7"),
        (1, 3, 3, "2021-07-07", 11, "bill7")
      )).toDF("order_id", "product_id", "seller_id", "date", "num_pieces_sold", "bill_raw_text")

      val quaterlySales = SalesDataAnalysis.totalSalesByQuarterly(salesData)
      quaterlySales.show(false)
      //      runningAvg.show(false)
    }
  }
}
