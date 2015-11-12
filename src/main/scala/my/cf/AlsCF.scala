package my.cf

/**
 * Created by jude on 15-11-10.
 */
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import math.sqrt
object AlsCF {

  val sconf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("user-cf")
    .set("spark.executor.memory", "4g")
  val sc =new SparkContext(sconf)

  def main (args: Array[String]){

    val ratings: RDD[Rating] = sc.textFile("sample_movielens_ratings.txt").map{
      _.split("::") match {
        case Array(uid,mid,rate,timestamp) =>
          Rating(uid.toInt, mid.toInt, rate.toDouble)
      }

    }
    val Array(train , test) = ratings.randomSplit(Array(0.7,0.3), 17)
//    train.take(10)
//    test.take(10)
    val rank = 1
    val numIteration = 10
    val model = ALS.train(train,rank, numIteration)
    val uidmid = test.map{ case Rating(uid, mid,rate)  =>
      (uid, mid)
    }
    val predictions = model.predict(uidmid).map{  case Rating(uid, mid,pred) =>
      ((uid, mid), pred)
    }
//    val predictions = test.map { case Rating(uid, mid,rate)  =>
//      val predict = model.predict(uid,mid)
//      ((uid, mid), predict)
//    }
    val withpredicts = test.map{ case Rating(uid, mid,rate) =>
      ((uid, mid), rate)

    }.join(predictions)

    val RMSE = withpredicts.map{ case ((uid, mid), (rate, predict)) =>
        val err = (rate - predict)
        sqrt(err * err)
    }.mean()

    println(s"RMSE is ", RMSE)

    sc.stop()
  }

}
