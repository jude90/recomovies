package my.cf

/**
 * Created by jude on 15-11-11.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix, IndexedRowMatrix, IndexedRow}

object ItemCF {
  val sconf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("item-one")
    .set("spark.executor.memory", "4g")
  implicit val sc =new SparkContext(sconf)
  def main(args: Array[String]): Unit = {

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


  // sample a user
    val user2pred = rows.takeSample(true,1)(0)
    println(user2pred.index)

    // item list of a sample user
    val prefs: SparseVector =  user2pred.vector.asInstanceOf[SparseVector]
    //
    val similar = Similarity(rows)
    val recommends = RecommendTop(prefs,similar)
    recommends.foreach(println)

  }
  def Similarity(rows: RDD[IndexedRow]): RDD[(Int, SparseVector)] ={
    val mat  = new IndexedRowMatrix(rows)

    //    compute similarity across columns
    val similar = mat.toRowMatrix().columnSimilarities(0.001)
    //    similar.entries.map

    // tranform to pair RDD wich key is item and value is vector of similarities with other item
      similar.toIndexedRowMatrix()
      .rows.map{ case IndexedRow(idx, vector) =>
      (idx.toInt, vector.asInstanceOf[SparseVector])
    }
  }
  def RecommendTop(userprefs:SparseVector,
                   similar:RDD[(Int, SparseVector)],
                   topk:Int =10,nums:Int=100)(implicit sc:SparkContext): Array[(Int,Double)] ={

    // key is items , values is preference of items
    val ipi:Array[(Int,Double)] =userprefs.indices zip userprefs.values
    // items that user has selected
    val uitems: Array[Int] = userprefs.indices
    val ij :RDD[(Int,Double)] = sc.parallelize(ipi)
      .join(similar.filter{case(i, vector)=> uitems.contains(i)})
      .flatMap{ case (i, (pi, vector:SparseVector)) =>
        (vector.indices zip vector.values)
        // filter item which has been in user's item list
        .filter{ case (item, pref)=> !uitems.contains(item) }
        // sort by Sim(i,j) and then take top k
        .sortBy{ case (j, sij) => sij}.reverse.take(topk)
          // item j with Sim(i,j) * Pi
          .map{ case (j, sij) => (j, sij * pi) }
      }
    // reduce by item j , select top nums Preference
    val result :Array[(Int,Double)] = ij.reduceByKey(_+_).sortBy(_._2,ascending = false).take(nums)

    result
  }
}
