package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}

object flatMapValues {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = sc.parallelize(list)

    rdd.foreach(println)
    val rdd1 = rdd.mapValues(x=>Seq(x,"male"))
    println("######################################## mapValues ")
    rdd1.foreach(println)
    println("######################################## flatMapValues ")
    //对[K,V]型数据中的V值  进行flatmap操作
    // 为了演示所以先将x 的值变成了一个集合
    val mapValuesRDD = rdd.flatMapValues(x=>Seq(x,"male"))
    mapValuesRDD.foreach(println)
  }
}
