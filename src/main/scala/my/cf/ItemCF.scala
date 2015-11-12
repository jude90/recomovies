package my.cf

/**
 * Created by jude on 15-11-11.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, IndexedRowMatrix, IndexedRow}

object ItemCF {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("item-one")
      .set("spark.executor.memory", "4g")
    val sc =new SparkContext(sconf)
    val ratings = sc.textFile("data/ratings.dat").map{ line:String =>
      val fields =line.split("::")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
//    ratings.map{ case( user, movie,_) =>
//      ((user, movie), 1)
//    }.reduceByKey(_+_).filter{ case (_, count)  => count > 1 }.foreach(println)

    val users = ratings.map{ case (user, _,_) =>
      user
    }

    val moives = ratings.map {case (_, moive, _) =>
      moive
    }

    val numUsers =users.max()
    val numMovies = moives.max()
//
    val rows = ratings.map{
      case (user, moive, rate) =>
      (user ,(moive, rate))
    }
      .groupByKey()
      .map{ case (key, values) =>
        Vectors.sparse(numMovies.toInt +1 , values.toSeq)
      }
    val mat  = new RowMatrix(rows)

    val similar = mat.columnSimilarities()
//    similar.entries.map
    val indexdsimilar = similar.toIndexedRowMatrix()
//    indexdsimilar(102)
//    similar.entries.foreach{ row =>
//      println(row.i, row.j)
//      print(row.j)
//    }
//    println(mat.numCols())
//    println(mat.numRows())
//    print(similar)
  }
}
