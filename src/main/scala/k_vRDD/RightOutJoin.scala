package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}

object RightOutJoin {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"),("C","C1"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    println("########################################")
    //RightOutJoin(otherDataSet, numPartitions):
    // 右外连接，包含右RDD的所有数据，如果左边没有与之匹配的用None表示,
    // numPartitions设置分区数，提高作业并行度
    val rightOutJoinRDD = rdd.rightOuterJoin(rdd1)
    rightOutJoinRDD.foreach(println)
    sc.stop
  }

}
