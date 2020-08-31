package com.kyrie.local

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by tend on 2020/8/26.
 */
object PersistRDDTest {


  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    sparkConf.set("spark.network.timeout","6000000")
    sparkConf.set("spark.executor.heartbeatInterval","5000000")
    val sc =new SparkContext(sparkConf)
    sc.setLogLevel("DEBUG")


    val rdd:RDD[String] = sc.textFile("data/wc.txt",2)

    val rdd2 = rdd.flatMap{line => line.split(" ")}

    val rdd3 = rdd2.map{word =>
      println("---------")
      word ->1
    }

    val rdd4 = rdd3.reduceByKey(_ + _)

    rdd4.cache

    rdd4.foreach(println)

    rdd4.foreach(println)

    println(rdd4.toDebugString)

    Thread.sleep(100000000)
  }

}
