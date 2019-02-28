package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object coalesce {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 16 ,4)  //这里就是装载 list
    println("coalesce从新分区前的数据集:"+rdd1.partitions.size)
    println("coalesce从新分区前的RDD依赖关系:"+rdd1.toDebugString)
    //把分区的值都打印出来
    val mapi1 = rdd1.mapPartitionsWithIndex{
      (partid,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[Int]]()
        var part_name = "part_" + partid
        part_map(part_name) = List[Int]()
        while(iter.hasNext){
          part_map(part_name) :+= iter.next()//:+= 列表尾部追加元素
        }
        part_map.iterator
      }
    }.collect

    mapi1.foreach(x => (println(x +" ")))
    println("##################################################")
    //对RDD的分区进行重新分区，shuffle默认值为false,当shuffle=false时，不能增加分区数
    //目,但不会报错，只是分区个数还是原来的
    //应用场景： 有时候需要重新设置Rdd的分区数量，比如Rdd的分区中，Rdd分区比较多，但是每个Rdd的数据量比较小，需要设置一个比较合理的分区。或者需要把Rdd的分区数量调大。
    // 还有就是通过设置一个Rdd的分区来达到设置生成的文件的数量。
    //repartition() 与 coalesce 相比，是shuffle = true 的情况
    //coalesce()方法的作用是返回指定一个新的指定分区的Rdd。
    //https://www.cnblogs.com/fillPv/p/5392186.html
    val coalesceRDD1 = rdd1.coalesce(2)  //当suffle的值为false时，不能增加分区数(即分区数不能从5->7)
    println("coalesce重新分区后的分区个数:"+coalesceRDD1.partitions.size)
    println("coalesce从新分区后的RDD依赖关系:"+coalesceRDD1.toDebugString)

    //把分区的值都打印出来
    val mapi = coalesceRDD1.mapPartitionsWithIndex{
      (partid,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[Int]]()
        var part_name = "part_" + partid
        part_map(part_name) = List[Int]()
        while(iter.hasNext){
          part_map(part_name) :+= iter.next()//:+= 列表尾部追加元素
        }
        part_map.iterator
      }
    }.collect

    mapi.foreach(x => (println(x +" ")))

    println("####################coalesce  shuffle = true 时#######################")
    val rdd = sc.parallelize(1 to 16,4)
    val coalesceRDD2 = rdd.coalesce(7,true)
    println("重新分区后的分区个数:"+coalesceRDD2.partitions.size)
    println("从新分区后的RDD依赖关系:"+coalesceRDD2.toDebugString)


  }

}
