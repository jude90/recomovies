package my.cf

/**
 * Created by jude on 15-11-11.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector
object ItemCF {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("item-one")
      .set("spark.executor.memory", "4g")
    val sc =new SparkContext(sconf)
    val ratings = sc.textFile("ratings.dat").map{ line:String =>
      val fields =line.split("::")
      (fields(1).toInt, fields(0).toInt)

    }


  }
}
