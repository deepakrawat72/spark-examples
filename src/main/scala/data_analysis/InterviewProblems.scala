package data_analysis

import org.apache.spark.sql.expressions.Window
import session.SparkSessionProvider
import org.apache.spark.sql.functions._

object InterviewProblems extends App with SparkSessionProvider {

  import spark.implicits._

  val temperatureList = List(
    (1, "2015–01–01", 10), (2, "2015–01–02", 25), (3, "2015–01–03", 20), (4, "2015–01–04", 30)
  )

  val tempDf = spark.sparkContext.parallelize(temperatureList).toDF("id", "record_date", "temperature")

  //find a day on which the weather temperature is higher than the previous day.
  tempDf.withColumn("prev_day_temperature", lag("temperature", 1, 0)
    .over(Window.orderBy("record_date")))
    .filter("temperature > prev_day_temperature")
    .drop("prev_day_temperature")
    .show(false)


  val purchasedItemsDf = spark.sparkContext.parallelize(List(
    ("banana", 2, "fruit"),
    ("apple", 8, "fruit"),
    ("leek", 2, "vegetable"),
    ("cabbage", 9, "vegetable"),
    ("lettuce", 10, "vegetable"),
    ("kale", 23, "vegetable")
  )).toDF("item", "purchases", "category")

  //Write a SQL to get the sum of total purchases by category?
  purchasedItemsDf.groupBy("category").agg(sum("purchases")).show(false)
  //Can you write a query to display the cumulative total purchases for each category
  purchasedItemsDf.withColumn("total_purchases",
    sum("purchases").over(Window.partitionBy("category").orderBy("purchases")))
    .show(false)

  //query to list out the student"s ID who has scored greater than 50 marks in all the subjects?
  val studentsDf = spark.sparkContext.parallelize(List(
    (1, "s1", 60), (1, "s2", 90), (1, "s3", 70), (2, "s1", 80), (2, "s2", 40), (2, "s3", 94), (3, "s1", 73), (3, "s2", 84), (3, "s3", 52)
  )).toDF("stud_id", "sub_id", "marks")

  studentsDf.filter("marks < 50").select("stud_id").distinct().show(false)

  val salesData = spark.sparkContext.parallelize(List(
    ("2012-10-13", "APAC", "XYZ", 500),
    ("2012-11-14", "APAC", "ABC", 600),
    ("2012-11-15", "APAC", "ABC", 200),
    ("2012-11-16", "APAC", "ABC", 50),
    ("2012-11-17", "APAC", "ABC", 270),
    ("2012-11-15", "South America", "ABC", 1100),
    ("2012-11-19", "South America", "BBC", 1200),
    ("2012-12-30", "South America", "ABC", 700),
    ("2012-11-12", "South America", "ABC", 800),
    ("2012-11-17", "South America", "BBC", 900),
    ("2012-12-16", "South America", "ABC", 100)
  )).toDF("date", "region", "product", "sales_amount")

  salesData.show(false)

  val rollingWindow = Window.partitionBy("region", "product").orderBy("date").rowsBetween(-2, Window.currentRow)

  salesData
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("rolling_average_3_days", avg("sales_amount").over(rollingWindow))
    .select("region", "product", "date", "sales_amount", "rolling_average_3_days")
    .show(false)


  //A company’s executives are interested in seeing who earns the most money in each of the company’s departments.
  // A high earner in a department is an employee who has a salary in the top three unique salaries for that department.
  // Write an SQL query to find the employees who are high earners in each of the departments.
  // Return the result table in any order.

  /**
   * Input:
   * Employee table:
   * +----+-------+--------+--------------+
   * | id | name  | salary | departmentId |
   * +----+-------+--------+--------------+
   * | 1  | Joe   | 85000  | 1            |
   * | 2  | Henry | 80000  | 2            |
   * | 3  | Sam   | 60000  | 2            |
   * | 4  | Max   | 90000  | 1            |
   * | 5  | Janet | 69000  | 1            |
   * | 6  | Randy | 85000  | 1            |
   * | 7  | Will  | 70000  | 1            |
   * +----+-------+--------+--------------+
   * Department table:
   * +----+-------+
   * | id | name  |
   * +----+-------+
   * | 1  | IT    |
   * | 2  | Sales |
   * +----+-------+
   * Output:
   * +------------+----------+--------+
   * | Department | Employee | Salary |
   * +------------+----------+--------+
   * | IT         | Max      | 90000  |
   * | IT         | Joe      | 85000  |
   * | IT         | Randy    | 85000  |
   * | IT         | Will     | 70000  |
   * | Sales      | Henry    | 80000  |
   * | Sales      | Sam      | 60000  |
   * +------------+----------+--------+
   */


  val empDf = spark.sparkContext.parallelize(List((1, "Joe", 85000, 1),
    (2, "Henry", 80000, 2),
    (3, "Sam", 60000, 2),
    (4, "Max", 90000, 1),
    (5, "Janet", 69000, 1),
    (6, "Randy", 85000, 1),
    (7, "Will", 70000, 1))).toDF("id","name","salary","departmentId")

  val deptDf = spark.sparkContext.parallelize(List((1, "IT"), (2, "Sales"))).toDF("id", "name")

  empDf.join(deptDf, empDf("departmentId") === deptDf("id"))
    .withColumn("ranked_salary", dense_rank().over(Window.partitionBy("departmentId").orderBy(desc("salary"))))
    .where("ranked_salary <= 3")
    .show(false)
}
