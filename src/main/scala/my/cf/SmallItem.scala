package my.cf

/**
 * Created by jude on 15-11-10.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SmallItem {
  val sconf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("small-item")
    .set("spark.executor.memory", "4g")
  val sc =new SparkContext(sconf)
  def main(args: Array[String]) {
    val small = sc.textFile("small.csv").map { line =>
     val fields:Seq[String] = line.split(",")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
    val reidx = small.map{ case(user, item, rate) =>
      (user, item)
    }
    val comatrix: RDD[((Int,Int), Int)] = reidx.join(reidx).map{ case(user, (item,item2)) =>
      ((item, item2), 1)
    }.reduceByKey(_ + _)

//    comatrix.collect()
//    comatrix.sortBy{case ((item, item2), num) =>
//        item
//    }.repartition(1).saveAsTextFile("comatrix.csv")

//    comatrix.collect()
//    reidx.collect()
//  print(reidx.collect())
  }
}
