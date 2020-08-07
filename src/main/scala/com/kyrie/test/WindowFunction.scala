package com.kyrie.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by tend on 2020/8/7.
 * spark 窗口函数测试
 */
object WindowFunction {

  def main(args: Array[String]) {


    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._







  }


  def anyliticFunction(): Unit ={

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val nbaDF = spark.read.csv("data/score.txt").toDF("name","age","score","district")


    //TODO 1.看看球员的东西部排名
    nbaDF.createOrReplaceTempView("nba_player")

    //1.spark sql实现
    val rankDF = spark.sql("SELECT * , " +
      "RANK() OVER (PARTITION BY district  ORDER BY score DESC) AS rank, " +
      "DENSE_RANK() OVER (PARTITION BY district ORDER BY score DESC) AS dense_rank, " +
      "ROW_NUMBER() OVER (PARTITION BY district ORDER BY score DESC) AS row_number " +
      "FROM nba_player")

    rankDF.show(20,false)


    // 2.DataFrame API 实现
    val windowSpec = Window.partitionBy("district").orderBy($"score".desc)

    nbaDF.select($"name",
      $"district",
      $"score",
      rank().over(windowSpec).as("rank"),
      dense_rank().over(windowSpec).as("dense_rank"),
      row_number().over(windowSpec).as("row_number")
    ).show(false)


    //TODO 2.球员联盟排名

    val allRankDF = spark.sql("SELECT * , " +
      "RANK() OVER (ORDER BY score DESC) AS rank, " +
      "DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank, " +
      "ROW_NUMBER() OVER (ORDER BY score DESC) AS row_number " +
      "FROM nba_player")

    allRankDF.show(20,false)




  }
}
