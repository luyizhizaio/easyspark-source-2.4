package com.kyrie.local

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by tend on 2020/8/2.
 */
object CheckpointRDD {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")

    val sc =new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    sc.setCheckpointDir("checkpoint")

    val rdd:RDD[String] = sc.textFile("data/test.txt")

    val rdd2 = rdd.flatMap{line => line.split(" ")}

    val rdd3 = rdd2.map{word =>
      println("---------")
      word ->1}

    //val rdd4 = rdd3.reduceByKey(_ + _)

    rdd3.cache.checkpoint()

    rdd3.foreach(println)

    rdd3.foreach(println)

    rdd3.foreach(println)


  }

}
