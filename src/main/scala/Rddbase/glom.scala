package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object glom {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 16,4)  //这里就是装载 list

    //将RDD的每个分区中的类型为T的元素转换换数组Array[T]
    val glomRDD = rdd.glom() //RDD[Array[T]]
    //getClass 获得类 Class.getSimpleName()方法。是获取源代码中给出的‘底层类’简称
    //Class.getName();以String的形式，返回Class对象的‘实体’名称
    glomRDD.foreach(rdd => println(rdd.getClass.getSimpleName))
    sc.stop

  }
}
