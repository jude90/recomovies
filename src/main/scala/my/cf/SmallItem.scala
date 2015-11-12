package my.cf

/**
 * Created by jude on 15-11-10.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors._
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, MatrixEntry}

object SmallItem {
  val sconf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("small-item")
    .set("spark.executor.memory", "4g")
  val sc =new SparkContext(sconf)
  def main(args: Array[String]) {
    val small = sc.textFile("data/ratings.dat").map { line =>
     val fields:Seq[String] = line.split(",")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
    val reidx = small.map{ case(user, item, rate) =>
      (user, item)
    }
    val comatrix: RDD[((Int,Int), Int)] = reidx.join(reidx).map{ case(user, (item,item2)) =>
      ((item, item2), 1)
    }.reduceByKey(_ + _)
    val rows = sc.parallelize(Array(dense(1,2,3,4),dense(2,0,-1,0)))
    val sim =new RowMatrix(rows).columnSimilarities()
    val inded = sim.toIndexedRowMatrix()
    sim.entries.foreach{ case MatrixEntry(i,j,v) =>
       println(i,j,v)
    }


    println(sim.numCols(), sim.numRows())
//    comatrix.collect()
//    comatrix.sortBy{case ((item, item2), num) =>
//        item
//    }.repartition(1).saveAsTextFile("comatrix.csv")

//    comatrix.collect()
//    reidx.collect()
//  print(reidx.collect())
  }
}
