package k_vRDD

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {

  // 具体详解看
  //http://www.cnblogs.com/MOBIN/p/5384543.html#1

  //comineByKey(createCombiner,mergeValue,mergeCombiners,partitioner,mapSideCombine)
  //createCombiner:在第一次遇到Key时创建组合器函数，将RDD数据集中的V类型值转换C类型值（V => C）
  //(x:Int) => (List(x),1)
  //mergeValue：合并值函数，再次遇到相同的Key时，将createCombiner 中的C类型值与这次传入的V类型值合并成一个C类型值（C,V）=>C，
  //(peo:(List[String],Int),x:String) => (List[String],Int)
  //mergeCombiners:合并组合器函数，将C类型值两两合并成一个C类型值
  //mapSideCombine：是否在map端进行Combine操作,默认为true


  //目标：统计男性和女生的个数，并以（性别，（名字，名字....），个数）的形式输出

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("combinByKey")
    val sc = new SparkContext(conf)
    val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))
    val rdd = sc.parallelize(people)

    val createCombiner = (x: String) => (List(x), 1)
    println("createCombiner 函数的作用 ：" + createCombiner("ni"))

    /* var mergeValue = (peo: (List[String], Int), x : String) => (x :: peo._1, peo._2 + 1)
     println("mergeValue 函数的作用 ："+mergeValue(createCombiner("ni"),"aa")*/

    /*  val mergeCombiners = (sex1: (List[String], Int), sex2: (List[String], Int)) => (sex1._1 ::: sex2._1, sex1._2 + sex2._2))*/
    //println("mergeCombiners 函数的作用 ："+mergeCombiners(createCombiner("ni"),createCombiner("ni"))

    rdd.foreach(println)
    println("########################################")
    //一个分区调用一次
        val combinByKeyRDD = rdd.combineByKey(
      (x: String) => (List(x), 1), // 传输(a, b) 默认list取第二个 于是就变成了（a，list(b)) a 可以是任意类型的，但是b一定是进入了list中 启发来源： https://blog.csdn.net/QQ1131221088/article/details/83896205
      (peo: (List[String], Int), x: String) => (x :: peo._1, peo._2 + 1), //結果是
      (sex1: (List[String], Int), sex2: (List[String], Int)) =>
        (sex1._1 ::: sex2._1, sex1._2 + sex2._2)
    )


    combinByKeyRDD.foreach(println)
    sc.stop()
  }

}
