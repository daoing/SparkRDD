package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {

  //按Key进行分组，返回[K,Iterable[V]]，numPartitions设置分区数，提高作业并行度
  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val list = List(("A",1),("B",2),("A",2),("B",3))
    val rdd = sc.parallelize(list)
    println("######################################## flatMapValues ")
    //按Key进行分组，返回[K,Iterable[V]]，numPartitions设置分区数，提高作业并行度
    //groupByKey(numPartitions)
    val groupByKeyRDD = rdd.groupByKey()
    groupByKeyRDD.foreach(println)
    sc.stop
  }
}
