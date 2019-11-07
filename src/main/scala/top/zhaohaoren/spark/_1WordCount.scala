package top.zhaohaoren.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _1WordCount {
  def main(args: Array[String]): Unit = {

    //    依据sparkContext构造器发现需要传入一个sparkConf对象
    //    这里设置spark运行环境为local环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("word_count")
    // 获取sc
    val sc = new SparkContext(conf)
    // 读取文件，将文件一行一行的读取
    // 该文件路径是依据当前部署环境有不同的协议写法：yarn的话应该hadoop上的hdfs:// 本地则是file://
    val lines: RDD[String] = sc.textFile("/Users/zhaohaoren/workspace/code/mine/JavaProjects/spark/src/main/resources/in")
    // word count
    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    // 输出结果
    res.collect().foreach(print)

  }
}
