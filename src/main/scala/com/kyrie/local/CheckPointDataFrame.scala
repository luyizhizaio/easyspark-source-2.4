package com.kyrie.local

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by tend on 2020/8/2.
 * 结论：
 * 1.不加checkpoint会重复执行
 * 2.checkpoint(false)：会执行两遍
 * 3.checkpoint()会执行两遍
 * 3.persist(StorageLevel.MEMORY_AND_DISK).checkpoint(false) 执行一遍
 */
object CheckPointDataFrame {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc =spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("checkpoint")
    import spark.implicits._

    val df = spark.read.text("data/test.txt")

    val df2 = df.flatMap{row =>
      println("---------------------")
      row.getString(0).split(" ")
      .map{word => word ->1}
    }.toDF().cache().checkpoint()

    df2.unpersist()

    Thread.sleep(100000000)

    println("count"+df2.count())

    println("fenge---")

    df2.show(false)


    df2.limit(3).show()


  }

}
