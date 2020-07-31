package com.kyrie.local

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

/**
 * Created by tend on 2020/7/31.
 */
object WordCount {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")

    val sc =new SparkContext(sparkConf)

    val rdd:RDD[String] = sc.textFile("data/wc.txt")

    val rdd2 = rdd.flatMap{line => line.split(" ")}

    val rdd3 = rdd2.map{word => word ->1}

    val rdd4 = rdd3.reduceByKey(_ + _)

    rdd4.foreach(println)

    Thread.sleep(100000000)

  }

}
