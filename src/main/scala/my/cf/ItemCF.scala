package my.cf

/**
 * Created by jude on 15-11-11.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix, IndexedRowMatrix, IndexedRow}
import util.Random
object ItemCF {
  val sconf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("item-one")
    .set("spark.executor.memory", "4g")
  implicit val sc =new SparkContext(sconf)
  implicit var sim :RDD[(Int,SparseVector)] = _
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
//    val user2pred = rows.takeSample(true,1)(0)
//    println(user2pred.index)

    // item list of a sample user
//    val prefs: SparseVector =  user2pred.vector.asInstanceOf[SparseVector]
    //
//    implicit val similar = Similarity(rows)
//    val recommends = RecommendTop(prefs)
//    recommends.foreach(println)

//    val Array(train, test) = rows.randomSplit(Array(0.6,0.4))
    val train = rows
    val test = rows.sample(true,0.1).collect()
    sim = Similarity(train)
    val (recall, precision )= PrecisionAndRecall(test)
    println(s"recall : ${recall}, precision: ${precision}")


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
  def RecommendTop(uservect:SparseVector,
                   topk:Int =10,nums:Int=10)
                  (implicit sc:SparkContext, similar:RDD[(Int, SparseVector)]): Array[(Int,Double)] ={

    // key is items , values is preference of items
    val ipi:Array[(Int,Double)] =uservect.indices zip uservect.values
    // items that user has selected
    val uitems: Array[Int] = uservect.indices
    val ij :RDD[(Int,Double)] = sc.parallelize(ipi)
      .join(similar.filter{case(i, vector)=> uitems.contains(i)})
      .flatMap{ case (i, (pi, vector:SparseVector)) =>
        (vector.indices zip vector.values)
        // filter item which has been in user's item list
        .filter{ case (item, _)=> !uitems.contains(item) }
        // sort by Sim(i,j) and then take top k
        .sortBy{ case (j, sij) => sij}.reverse.take(topk)
          // item j with Sim(i,j) * Pi
          .map{ case (j, sij) => (j, sij * pi) }
      }
    // reduce by item j , select top nums Preference
    val result :Array[(Int,Double)] = ij.reduceByKey(_+_).sortBy(_._2,ascending = false).take(nums)

    result
  }


  def PrecisionAndRecall(test_set:Array[IndexedRow],
                          K:Int=10,N:Int=10):(Double,Double)={
//    val train_join_test= train_set
//      .map{ case IndexedRow(index,vector) =>(index, vector.asInstanceOf[SparseVector])}
//      .join(test_set.map{ case IndexedRow(index,vector) =>(index, vector.asInstanceOf[SparseVector])})
//

    var recall:Double = 0
    var precision:Double = 0
    var hit:Double = 0
    test_set.map{ case IndexedRow(index,vector) =>(index, vector.asInstanceOf[SparseVector])}
      .foreach{ case (uid,  vect)=>
        // shuffled  item list before split
        val shuffled = Random.shuffle(vect.indices zip vect.values).asInstanceOf[Array[(Int,Double)]]
        // split a item vector into two exclusive part
        val (predict_set:Array[(Int,Double)], vali_set:Array[(Int,Double)]) = shuffled.splitAt(vect.indices.length.toInt/2)

//        reform the predict_set into a SparseVector
        val predicts =Vectors.sparse(predict_set.length, predict_set.toSeq).asInstanceOf[SparseVector]

        val recommends = RecommendTop(predicts,K,N)
        // hits mean intersection of test list and recommend list
      val hits = vali_set.indices.toSet intersect recommends.map(_._1).toSet
      hit += hits.size
      recall += vali_set.size
      precision += N

    }
    ( hit/recall, hit/precision )
  }
}
