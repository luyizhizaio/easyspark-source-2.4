package com.kyrie.first

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Kyrie on 2019/8/4.
 */
object FirstRemote {

  var appName = "FirstRemote"

  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME","hadoop")
    val sc = sparkContext()
    val rdd = sc.textFile("hdfs://192.168.1.61:9000/data.txt")

    val rdd2 = rdd.map{word => word -> 1 }.reduceByKey(_ + _)


    rdd2.saveAsTextFile("hdfs://192.168.1.61:9000/data_out")


  }

  def sparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster("spark://192.168.1.61:7077")

    //设置保存历史日志
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.history.fs.logDirectory","hdfs://192.168.1.61:9000/spark/log/historyEventLog")
    conf.set("spark.eventLog.dir","hdfs://192.168.1.61:9000/spark/log/eventLog")

    conf.set("spark.driver.memory","400m")
    conf.set("spark.executor.memory","500m")
    conf.set("","")
    conf.set("spark.executor.cores","1")

    val sc = new SparkContext(conf)

    //设置日志级别
    sc.setLogLevel("INFO")
    sc
  }

}
