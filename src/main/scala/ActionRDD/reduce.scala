package ActionRDD

import org.apache.spark.{SparkConf, SparkContext}

object reduce {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Action")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10,2)


    //reduce(func):通过函数func先聚集各分区的数据集，
    // 再聚集分区之间的数据，func接收两个参数，
    // 返回一个新值，新值再做为参数继续传递给函数func，直到最后一个元素
    val reduceRDD = rdd.reduce(_ + _)
    val reduceRDD1 = rdd.reduce(_ - _) //如果分区数据为1结果为 -53
    val countRDD = rdd.count()
    val firstRDD = rdd.first()
    val takeRDD = rdd.take(5)    //输出前个元素
    val topRDD = rdd.top(3)      //从高到底输出前三个元素 降序TOPN
    val takeOrderedRDD = rdd.takeOrdered(3)    //按自然顺序从底到高输出前三个元素 升序TOPN

    println("func +: "+reduceRDD)
    println("func -: "+reduceRDD1)
    println("count: "+countRDD)
    println("first: "+firstRDD)
    println("take:")
    takeRDD.foreach(x => print(x +" "))
    println("\ntop:")
    topRDD.foreach(x => print(x +" "))
    println("\ntakeOrdered:")
    takeOrderedRDD.foreach(x => print(x +" "))
    sc.stop
  }

}
