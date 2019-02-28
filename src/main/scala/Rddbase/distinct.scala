package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object distinct {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val list = List(1,2,3,4,4,5,3,5)
    val rdd1 = sc.parallelize(list)  //这里就是装载 list

    //对RDD中的元素进行去重
    val distinctRDD = rdd1.distinct()
    distinctRDD.collect.foreach(x => print(x + " "))
    sc.stop
  }

}
