package ActionRDD

import org.apache.spark.{SparkConf, SparkContext}

object countByKey {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("KVFunc")
    val sc = new SparkContext(conf)
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr,2)
    //countByKey():作用于K-V类型的RDD上，统计每个key的个数，返回(K,K的个数)
    val countByKeyRDD = rdd.countByKey()
    //collectAsMap():作用于K-V类型的RDD上，作用与collect不同的是collectAsMap函数不包含重复的key，
    // 对于重复的key。后面的元素覆盖前面的元素
    val collectAsMapRDD = rdd.collectAsMap()
    //.lookup(k)：作用于K-V类型的RDD上，返回指定K的所有V值
    val lookupRDD = rdd.lookup("A")

    println("lookup:")
    lookupRDD.foreach(x => println(x+" "))

    println("countByKey:")
    countByKeyRDD.foreach(print)

    println("\ncollectAsMap:")
    collectAsMapRDD.foreach(print)
    sc.stop
  }

}
