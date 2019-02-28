package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object cartesian {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 3)  //这里就是装载 list
    val rdd2 = sc.parallelize(2 to 5)  //这里就是装载 list

    //笛卡尔积
    val cartesianRDD = rdd1.cartesian(rdd2)
    cartesianRDD.collect.foreach(x => print(x + " "))
    //cartesianRDD.collect.foreach(println)  //这样和上述一个效果
    sc.stop
  }

}
