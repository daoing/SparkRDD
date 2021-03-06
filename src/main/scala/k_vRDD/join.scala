package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}

object join {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)

    val list1 = List(("A",1),("B",2),("A",2),("B",3))
    val list2 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd1 = sc.parallelize(list1,3)
    val rdd2 = sc.parallelize(list2,3)
    println("########################################")
    //join(otherDataSet,numPartitions):对两个RDD先进行cogroup操作形成新的RDD，
    // 再对每个Key下的元素进行笛卡尔积，numPartitions设置分区数，提高作业并行度
    val groupByKeyRDD = rdd1.join(rdd2)
    groupByKeyRDD.foreach(println)
    sc.stop
  }

}
