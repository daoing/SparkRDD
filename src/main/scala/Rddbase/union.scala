package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object union {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 5)  //这里就是装载 list
    val rdd2 = sc.parallelize(5 to 10)  //这里就是装载 list

    //将两个RDD的数据集进行合并，最终返回两个RDD的并集，若RDD中存在相同的元素也不会去重
    val unionRDD = rdd1.union(rdd2)
    unionRDD.collect.foreach(x => print(x + " "))
    sc.stop
  }

}
