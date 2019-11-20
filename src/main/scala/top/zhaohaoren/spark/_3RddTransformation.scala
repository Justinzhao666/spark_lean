package top.zhaohaoren

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object _3RddTransformation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("transformation")
    val sc = new SparkContext(conf)

    val listRdd = sc.makeRDD(1 to 10)
    //map算子
    val mapRdd: RDD[Int] = listRdd.map(_ * 2)
    mapRdd.collect()

    //mapPartitions算子，将一个RDD 分区的所有数据 全部传到executor执行，而map是将RDD的数据每一个。
    //所以他比map 减少网络IO开销，但是可能有RDD区的内存占用大 executor撑不住的问题
    val mapPartitionsRdd: RDD[Int] = listRdd.mapPartitions(datas => {
      datas.map(_ * 2) // 将一个分区的数据全部都x2。 这个整个map是scala的执行，这个{xx.map}是spark的一个计算。
    })
    mapPartitionsRdd.collect().foreach(println)

    listRdd.mapPartitionsWithIndex { // 使用模式匹配 一般用{}
      case (no, datas) => { // no是分区号
        datas.map((_, "分区号" + no)) // 需求是：知道每个数据在哪个分区号？ 所以对每个分区的每个数据 生成一个新的tuple，设置（数据，分区号）这个分区号是spark自己生成的。
      }
    }

    //flat map
    val listRdd2: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
    listRdd2.flatMap(x => x).collect().foreach(println)

    // glom的用处：需要对每个分区的数据 局部处理（替代mapPartition）。
    val listRdd3: RDD[Int] = sc.makeRDD(1 to 16, 4) // 1-16 分4个分区 (这个分区如果不能均分的话，多余的数据都会分布在后面的那些分区)
    val glomRdd: RDD[Array[Int]] = listRdd3.glom() // 将一个分区的数据 放到一个数组中
    glomRdd.collect().foreach(array => println(array.mkString(",")))

    // group by 按照指定规则进行分组，这个规则，根据数据生成一个key，然后按照这个key来进行分组
    val groupRdd: RDD[(Int, Iterable[Int])] = listRdd.groupBy(x => x % 2) // 返回 （key，组）

    // filter过滤
    val filterRdd: RDD[Int] = listRdd.filter(x => x > 2)
    filterRdd.collect().foreach(println)

    // sample 数据采样  从数据中抽取部分来做数据采样。
    // 参数
    // withReplacement: 抽取的采样数据是否需要放回原始数据中，来进行下一次抽取。true和false 决定  PoissonSampler 泊松分布，BernoulliSampler 非正即反 使用哪种算法采样。
    // fraction: 打分机制： 设置一个分值，Double, 在0~1之间的一个浮点值，表示要采样的记录在全体记录中的比例。 seed会为rdd中的每一个数随机打一个分， 然后将小于这个数字的全筛选出来。所以0就全不筛选出来（没有比0小的），1就是全部筛选出来。【具体看注释】
    // seed: 随机数生成种子,用算法进行算出的结果为伪随机数。
    val sampleRdd: RDD[Int] = listRdd.sample(false, 1, 1)
    sampleRdd.collect().foreach(println)

    // 去重 distinct
    // 去重后数据会被打乱
    val tinctRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 3, 1, 6))
    val distinctRdd: RDD[Int] = tinctRdd.distinct()
    //    val distinctRdd: RDD[Int] = tinctRdd.distinct(2) 指定去重结果存放的 分区数量。 一般因为去重后数据数量都会减少，所以一般都会指定分区数量。
    distinctRdd.collect().foreach(println)

    // 缩减 coalesce
    // 缩减分区数，用于大数据集过滤后提高小数据集的执行效率
    // 所谓的缩减其实就是 区和区之间的合并。 尽量合并后面的区。
    val listRdd4: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println("缩减分区前的数量" + listRdd4.partitions.size)
    val coalRdd: RDD[Int] = listRdd4.coalesce(3)
    println("缩减分区后数量" + coalRdd.partitions.size)

    // 打乱重组 repartition
    // 使用上面coalesce的分区合并，很容易导致数据倾斜。2个大的分区合并在了一起就 整体倾斜了。
    // 使用repartition可以 将RDD打乱后重组。 其实底层本质也是调用的coalesce，只是有个参数：是否shuffle？默认为true了。
    val listRdd5: RDD[Int] = sc.makeRDD(1 to 16, 4)
    listRdd5.repartition(2)


    // sort by 排序
    val sortRdd: RDD[Int] = listRdd5.sortBy(x => x, /*排序规则*/ false /*排序方向*/)

    // union 并集 subtract 差集 intersection 交集  cartesian 笛卡尔积 zip 2个数据集两两配对。  配对数量不够的话，和scala不一样spark算子会直接报错，2个数据集数量应该是一样的分区数也要相等。
    val rdd1: RDD[Int] = sc.makeRDD(1 to 10)
    val rdd2: RDD[Int] = sc.makeRDD(10 to 20)
    val rddUnion: RDD[Int] = rdd1.union(rdd2)


    /*KV 一个Pair类型的RDD  转换*/
    // 他们都有一个特质： 无论后面算子是怎么算的，都是对key进行分组后的。

    // 分区 partitionBy
    val pairRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 2), ("c", 2)))
    pairRdd.partitionBy(new MyPartitioner(3)) // 使用自定义分区来进行分区，传入一个分区器。
    pairRdd.glom().foreach(println)

    // 分组 groupByKey 按照key进行分组
    val groupPairRdd: RDD[(String, Iterable[Int])] = pairRdd.groupByKey()

    // reduce by key 按照key进行分组进行reduce
    val reducePairRdd: RDD[(String, Int)] = pairRdd.reduceByKey(_ + _)

    // aggregateByKey  分区内一个操作，分区和分区间 又是指定另外一个操作。
    val pairPartitionRdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 2), ("c", 2)), 2)
    val aggRdd: RDD[(String, Int)] = pairPartitionRdd.aggregateByKey(0 /*需要一个初始值（对key的初始值），因为reduce的操作的算子函数是两两进行的，第一个值需要和一个初始值来进行计算*/)(
      math.max(_, _) /* 第一个函数：区间内是怎么运算的*/ ,
      _ + _ /*第二个函数：区间间是怎么运算的*/)


  }
}

class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    // 分了几个区域
    partitions
  }

  override def getPartition(key: Any): Int = {
    1 // 这个key表示我要根据kv中k的那个数据 来决定分到哪个区。
  }
}
