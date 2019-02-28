package ActionRDD

import org.apache.spark.{SparkConf, SparkContext}

object fold {

  //-Xms256m -Xmx1024m
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Fold")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)), 2)
    //fold(zeroValue:T)(op:(T,T) => T):通过op函数聚合各分区中的元素及合并各分区的元素，
    // op函数需要两个参数，在开始时第一个传入的参数为zeroValue,
    // T为RDD数据集的数据类型，其作用相当于SeqOp和comOp函数都相同的aggregate函数
    //其过程如下：
    //1.开始时将(“d”,0)作为op函数的第一个参数传入，将Array中和第一个元素("a",1)作为op函数的第二个参数传入，并比较value的值，返回value值较大的元素
    //2.将上一步返回的元素又作为op函数的第一个参数传入，Array的下一个元素作为op函数的第二个参数传入，比较大小
    //3.重复第2步骤
    //
    //每个分区的数据集都会经过以上三步后汇聚后再重复以上三步得出最大值的那个元素，对于其他op函数也类似，只不过函数里的处理数据的方式不同而已
    val foldRDD = rdd.fold(("d", 0))((val1, val2) => { if (val1._2 >= val2._2) val1 else val2 })
    println(foldRDD)
  }


}
