package com.kyrie.dataset

import org.apache.spark.sql.SparkSession

/**
 * Created by Kyrie on 2019/5/12.
 */
object DataSetHead {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()

    val ds = spark.read.text("data/test.txt")

    ds.explain(true)

    ds.head(2).foreach(println)




  }

}
