package com.kyrie.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 * Created by tend on 2020/8/13.
 */
object NestStructureTest {


  def main(args: Array[String]) {


    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._




    val df = spark.sql(
      """
        |select user_id, user_loans_arr, new_loan
        |from values
        | ('u1', array(named_struct('loan_date', '2019-01-01', 'loan_amount', 100)), named_struct('loan_date',
        | '2020-01-01', 'loan_amount', 100)),
        | ('u2', array(named_struct('loan_date', '2020-01-01', 'loan_amount', 200)), named_struct('loan_date',
        | '2020-01-01', 'loan_amount', 100))
        | T(user_id, user_loans_arr, new_loan)
      """.stripMargin)
    df.show(false)
    df.printSchema()

//    +-------+-------------------+-----------------+
//    |user_id|user_loans_arr     |new_loan         |
//    +-------+-------------------+-----------------+
//    |u1     |[[2019-01-01, 100]]|[2020-01-01, 100]|
//    |u2     |[[2020-01-01, 200]]|[2020-01-01, 100]|
//    +-------+-------------------+-----------------+

    // spark >= 2.4
    df.withColumn("user_loans_arr",
      expr(
        """
          |FILTER(array_union(user_loans_arr, array(new_loan)),
          | x -> months_between(current_date(), to_date(x.loan_date)) < 12)
        """.stripMargin))
      .show(false)


    // spark < 2.4
    val outputSchema = df.schema("user_loans_arr").dataType

    import java.time._
    val add_and_filter = udf((userLoansArr: mutable.WrappedArray[Row], loan: Row) => {
      (userLoansArr :+ loan).filter(row => {
        val loanDate = LocalDate.parse(row.getAs[String]("loan_date"))
        val period = Period.between(loanDate, LocalDate.now())
        period.getYears * 12 + period.getMonths < 12
      })
    }, outputSchema)

    df.withColumn("user_loans_arr", add_and_filter($"user_loans_arr", $"new_loan"))
      .show(false)



//    +-------+--------------------------------------+-----------------+
//    |user_id|user_loans_arr                        |new_loan         |
//    +-------+--------------------------------------+-----------------+
//    |u1     |[[2020-01-01, 100]]                   |[2020-01-01, 100]|
//    |u2     |[[2020-01-01, 200], [2020-01-01, 100]]|[2020-01-01, 100]|
//    +-------+--------------------------------------+-----------------+

  }

}
