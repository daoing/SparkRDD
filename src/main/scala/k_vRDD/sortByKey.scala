package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}

object sortByKey {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)

    val list = List(("A",1),("B",2),("A",2),("B",3))
    val rdd = sc.parallelize(list)
    println("########################################")
    //sortByKey(accending，numPartitions):返回以Key排序的（K,V）键值对组成的RDD，
    // accending为true时表示升序，false时表示降序
    // 为numPartitions设置分区数，提高作业并行度
    val sortByKeyRDD = rdd.sortByKey()
    sortByKeyRDD.foreach(println)
    sc.stop
  }

}
