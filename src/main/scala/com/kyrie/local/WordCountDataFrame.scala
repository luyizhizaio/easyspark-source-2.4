package com.kyrie.local

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by tend on 2020/7/31.
 */
object WordCountDataFrame {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    sparkConf.set("spark.network.timeout","600")
    sparkConf.set("spark.executor.heartbeatInterval","500")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.text("data/wc.txt")

    val num = df.count()

    println(num)

    df.explain(true)

    Thread.sleep(100000000)

  }

}
