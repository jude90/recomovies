package my.cf

/**
 * Created by jude on 15-11-11.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
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
    val rows:RDD[IndexedRow] = ratings.map{
      case (user, moive, rate) =>
      (user ,(moive, rate))
    }
      .groupByKey()
      .map{ case (key, values) =>
       IndexedRow(key, Vectors.sparse(numMovies.toInt +1 , values.toSeq))
      }

    // index RowMat which idx is a user and vector is items of the user
    val mat  = new IndexedRowMatrix(rows)


//    compute similarity across columns
    val similar = mat.toRowMatrix().columnSimilarities()
//    similar.entries.map

    // tranform to pair RDD wich key is item and value is vector of similarities with other item
    val indexdsimilar = similar.toIndexedRowMatrix()
      .rows.map{ case IndexedRow(idx, vector) =>
      (idx.toInt, vector)
    }



  // sample a user
    val user2pred = mat.rows.takeSample(true,1)(0)
    println(user2pred.index)

    // item list of a sample user
    val prefs: SparseVector =  user2pred.vector.asInstanceOf[SparseVector]

    // key is items , values is preference of items
    val ipi = (prefs.indices zip prefs.values)
    val k = 10
    // items that user has selected
    val uitems = prefs.indices
    //
    val ij = sc.parallelize(ipi)
      .join(indexdsimilar)
      .flatMap{ case (i, (pi, vector:SparseVector)) =>
      // item j with Sim(i,j) * Pi

      (vector.indices zip vector.values)
        // filter item which has been in user's item list
        .filter{ case (item, pref)=> !uitems.contains(item) }
        // sort by Sim(i,j) and then take top k
        .sortBy{ case (j, sij) => sij}.reverse.take(k)
        .map{ case (j, sij) => (j, sij * pi) }
    }
    val result = ij.reduceByKey(_+_).sortBy(_._2,ascending = false).take(100)
    result.foreach(println)

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
