package com.kyrie.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by tend on 2019/8/15.
 */

object FirstTest {

  case class Test(a: Int = Random.nextInt(1000000),
                  b: Double = Random.nextDouble,
                  c: String = Random.nextString(1000),
                  d: Seq[Int] = (1 to 100).map(_ => Random.nextInt(1000000))) extends Serializable

  def main(args: Array[String]) {


    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[Test]))
    val spark = SparkSession.builder().config(sparkConf).master("local").getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val start = System.currentTimeMillis()
    val input = sc.parallelize(1 to 1000000, 42).map(_ => Test()).persist(StorageLevel.DISK_ONLY)
    input.count() // Force initialization
    val shuffled = input.repartition(43).count()

    println(input.toDebugString)

    val end = System.currentTimeMillis()
    println((end - start)/1000)
  }


}
