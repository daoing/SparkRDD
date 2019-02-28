package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}


//foldByKey函数是通过调用CombineByKey函数实现的
//目标：先让相同Key的value值相加，然后相同的key合并在一起，然后在使得他们的v + 2
//举例 ：("Mobin", 2), ("Mobin", 1) key相同 合并相加 =》("Mobin", 3) 然后在对v 加2 =》("Mobin", 5)
//当createCombiner，mergeValue和mergeCombiners函数操作都相同, 唯独需要一个zeroValue时, 适用
object foldByKey {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)

    val people = List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Amy", 1), ("Lucy", 3))
    val rdd = sc.parallelize(people)
    println("########################################")
    //
    val foldByKeyRDD = rdd.foldByKey(2)(_+_)
    foldByKeyRDD.foreach(println)

  }

}
