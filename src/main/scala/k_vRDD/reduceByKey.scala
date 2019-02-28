package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)

    val arr = List(("A",3),("A",2),("B",1),("B",3))
    val rdd = sc.parallelize(arr)
    val reduceByKeyRDD = rdd.reduceByKey(_ +_)

    println("########################################")
    //按Key进行分组，使用给定的func函数聚合value值, numPartitions设置分区数，提高作业并行度
    reduceByKeyRDD.foreach(println)
    sc.stop

  }

}
