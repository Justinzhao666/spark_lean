package top.zhaohaoren.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object _2RddCreate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // 创建RDD的方式1：从内存集合
    // 内部实现其实就是sc.parallelize()方法
    val listRdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 创建RDD的方式2：从外部文件
    // 可以设置最小分区参数，因为文件我们制定目录的话，不知道有多少个。最小分区可以对数据进行分片比如数据5个，最小设置为2，那么最后就分2，2，1 三片。
    // 但是最后分成三个片的文件内容不一定是按照分片的规则来的，而是按照hadoop的分片规则来的。所以12345可能都写在一个文件上，因为这是两个不同的过程。
    // 具体分片规则，是hadoop会一行行读，读第一行判断是否大于2，大于2就写入这一行。所以最后结果有3个part，但是只有part1有内容。
    val fileRdd: RDD[String] = sc.textFile("in",2)
    fileRdd.saveAsTextFile("output")
  }
}
