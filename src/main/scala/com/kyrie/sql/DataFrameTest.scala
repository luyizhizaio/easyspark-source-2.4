package com.kyrie.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by tend on 2020/7/31.
 */
object DataFrameTest {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    sparkConf.set("spark.network.timeout","600")
    sparkConf.set("spark.executor.heartbeatInterval","500")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val df = spark.read.text("data/wc.txt")

    df.explain(true)

    val df2 = df.flatMap{row =>
      row.getString(0).split(" ")
    }.toDF("word").groupBy("word").count()

    df2.show(false)

    df2.explain(true)

   Thread.sleep(100000000)


  }

}
