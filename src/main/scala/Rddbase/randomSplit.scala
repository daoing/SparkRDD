package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object randomSplit {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10)  //这里就是装载 list

    //根据weight权重值将一个RDD划分成多个RDD,权重越高划分得到的元素较多的几率就越大
    val randomSplitRDD = rdd.randomSplit(Array(1.0,2.0,7.0))
    randomSplitRDD(0).foreach(x => print(x +" "))
    println()
    randomSplitRDD(1).foreach(x => print(x +" "))
    println()
    randomSplitRDD(2).foreach(x => print(x +" "))
    sc.stop
  }


}
