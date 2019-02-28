package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object intersection {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 5)  //这里就是装载 list
    val rdd2 = sc.parallelize(5 to 10)  //这里就是装载 list

    //返回两个RDD的交集
    val unionRDD = rdd1.intersection(rdd2)
    unionRDD.collect.foreach(x => print(x + " "))
    sc.stop
  }

}
