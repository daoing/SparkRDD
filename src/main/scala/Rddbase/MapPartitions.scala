package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  //定义函数
  def partitionsFun(/*index : Int,*/iter : Iterator[(String,String)]) : Iterator[String] = {
    var woman = List[String]()   //创建一个空的list 集合
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = /*"["+index+"]"+*/next._1 :: woman   //x::list,其中x为加入到头部的元素 并返回新的列表  //生成一个list集合
        case _ =>
      }
    }
    return  woman.iterator  //返回一个list 的迭代器
  }

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
   val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val l = List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female"))
    val rdd = sc.parallelize(l,2)  //这里就是装载 list

    /* val mp = rdd.mapPartitions(partitionsFun)  //mp 是一个 list 的迭代器 每一个分区都这样做 到一个分区做一次操作 与  numslices 的个数有关
    /*val mp = rdd.mapPartitionsWithIndex(partitionsFun)*/
    mp.collect.foreach(x => (print(x +" ")))   //将分区中的元素转换成Aarray再输出  收集然后遍历输出*/

    val mp1 = rdd.mapPartitions(x => x.filter(_._2 == "female")).map(x => x._1)   //_._2 每个元素去第二个元素
    mp1.collect.foreach(x => (print(x +" ")))
  }
}
