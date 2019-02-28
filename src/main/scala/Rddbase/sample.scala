package Rddbase

import org.apache.spark.{SparkConf, SparkContext}

object sample {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitions")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10)  //这里就是装载 list

    //https://blog.csdn.net/kaede1209/article/details/81145560
    //第一个参数指定是否放回，第二个参数指定抽取的数量（按百分比计算），第三个指定随机种子(暂时随便写，目的是为了更有随机性)
    //作用是 避免数据倾斜  三个参数组合不一样，结果都不一样，我们只用知道会经过随机给我们返回一些值即可，就这么个作用。
    //当极个别的task数据倾斜，并且量非常大 通过sample采样，得到倾斜的key
    val sample =  rdd.sample(true,0.5,1  )
    sample.collect.foreach(x => print(x + " "))
    sc.stop
  }

}
