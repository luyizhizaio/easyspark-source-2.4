package com.kyrie.local

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

/**
 * Created by tend on 2020/7/31.
 */
object WordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    sparkConf.set("spark.network.timeout","600")
    sparkConf.set("spark.executor.heartbeatInterval","500")
    val sc =new SparkContext(sparkConf)

    val rdd:RDD[String] = sc.textFile("data/wc.txt",4) //HadoopRDD

    val rdd2 = rdd.flatMap{line => line.split(" ")}//MapPartitionsRDD

    val rdd3 = rdd2.map{word => word ->1} //MapPartitionsRDD

    val rdd4 = rdd3.reduceByKey(_ + _) //ShuffledRDD

    println(rdd4.toDebugString)

    //触发job执行
    rdd4.foreach(println)

    Thread.sleep(100000000)

  }

}
