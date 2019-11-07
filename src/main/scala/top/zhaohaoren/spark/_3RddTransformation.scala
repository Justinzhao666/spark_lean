package top.zhaohaoren

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _3RddTransformation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("transformation")
    val sc = new SparkContext(conf)

    val listRdd = sc.makeRDD(1 to 10)
    //map算子
    val mapRdd: RDD[Int] = listRdd.map(_ * 2)
    mapRdd.collect()
  }
}
