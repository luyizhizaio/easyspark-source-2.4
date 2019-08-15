package com.kyrie.local

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/**
  * Created by tend on 2019/8/15.
  */

object FirstTest2 {

   case class Test(a: Int = Random.nextInt(1000000),
                   b: Double = Random.nextDouble,
                   c: String = Random.nextString(1000),
                   d: Seq[Int] = (1 to 100).map(_ => Random.nextInt(1000000))) extends Serializable

   def main(args: Array[String]) {


     val sparkConf = new SparkConf()
     val spark = SparkSession.builder().config(sparkConf).master("local").getOrCreate()
     import spark.implicits._

     val sc = spark.sparkContext

     val start = System.currentTimeMillis()

     val input = sc.parallelize(1 to 100000, 42).map(_ => Test()).toDS.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
     input.count() // Force initialization
     val shuffled = input.repartition(43).count()

     println(input.rdd.toDebugString)

     val end = System.currentTimeMillis()
     println((end - start)/1000)
   }


 }
