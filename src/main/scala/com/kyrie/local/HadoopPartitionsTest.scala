package com.kyrie.local

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by tend on 2020/8/6.
 * 研究
 * 1）读取hdfs文件的分区设置，2）分区数据如何读取
 */
object HadoopPartitionsTest {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\hadoop")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    sparkConf.set("spark.network.timeout","600")
    sparkConf.set("spark.executor.heartbeatInterval","500")
    val sc =new SparkContext(sparkConf)


    /*

    情况一：
  文件内容：
  dayue
  et
  1）计算分区：
    总字节数：8；分区为2.
    目标分区大小： goalSize = 8/2 =4
    生成两个分区：
    0，0+4
    4，4+4
  2）分区读取内容：
    按行读取
    所以第一个分区：读取第一行
    第二个分区：读取第二行


    情况二：
    文件内容：
     da
     yue et
     1）计算分区：
      总字节数：9；分区为2.
      目标分区大小： goalSize = 9/2 =4 余1； 1/4>0.1,所以会生成三个分区
        0，4
        4，8
        8，9
     2)分区读取内容
      按行读取
      第一个分区读取0到4这5个字节，所以把两行内容都读取了。第2，3两个分区为空。


      情况三：
      文件内容：
        d
        a
        y
        u
        e
        e
        t
      1）计算分区：
      总字节数：9；指定预分区为3.
      目标分区大小： goalSize = 13/3 =4， 余1； 1/4>0.1,所以会生成4个分区
        0，4
        4，8
        8，12
        12, 13
     2)分区读取内容
      按行读取
      文件内容的offset为：
      0 1
      2 3
      4 5
      6 7
      8 9
      10 11
      12

       第1个分区读取：0到4，读取前3行
       第2个分区读取：4到8，因为4，5被读去了，从来6开始读到9。（因为按行读取所以读到9）
       第3个分区读取：8到12，因为8，9被读去了，从来10开始读到12。


      第一个分区读取0到4这5个字节，所以把两行内容都读取了。第2，3两个分区为空。




     */


    val df = sc.textFile("data/wc.txt")

    df.saveAsTextFile("data/wc.out2")


  }


}
