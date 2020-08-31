package com.kyrie.local

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by tend on 2020/8/14.
 */
object RDDPipelineAnalyse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("检测spark数据处理pipeline")
      .master("local").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val rdd01 = sc.parallelize(Array((1,1),(2,1),(3,1),(4,1),(5,1),(6,1)),2)
    //val rdd02 = sc.parallelize(Array((1,100),(2,100),(3,100),(4,100),(5,100),(6,100)),1)
    val rdd1 = rdd01.map{ x => {
      println("map01--------"+x)
      x
    }}
    val rdd_1 = rdd1.filter(x=>{
      println("filter01--------"+x)
      true
    })
//    val rdd2 = rdd02.map{ x => {
//      println("map02--------"+x)
//      x
//    }}
    val rdd_2 = rdd1.filter(x=>{
      println("filter02--------"+x)
      true
    })
    val rdd3: RDD[(Int, (Int, Int))] = rdd_1.join(rdd_2)

    val rdd4 = rdd3.map(x=>{
      println("map01 join map02--------"+x)
      x
    })

    rdd4.collect()
    sc.stop()

  }

}