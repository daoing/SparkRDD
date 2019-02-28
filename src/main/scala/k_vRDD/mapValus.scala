package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}


object mapValus {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = sc.parallelize(list)

    rdd.foreach(println)
    println("########################################")
    //对[K,V]型数据中的V值 进行map操作
    val mapValuesRDD = rdd.mapValues(_+2)
    mapValuesRDD.foreach(println)
  }
}
