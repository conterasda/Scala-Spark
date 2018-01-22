import com.mongodb.spark.config.ReadConfig
import merchant.configuration.BaseContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.util.JSONParser

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.{JSONArray, JSONObject}

class MongoRead extends BaseContext with Serializable {
  def readDB(sc: SparkContext, database: String,
             collection: String, queries: String): MongoRDD[Document] = {
    val readConfig = ReadConfig(Map("uri" ->
      ("mongodb://localhost:27017/" + database + "." + collection)))
    //+ "?replicaSet=PMArepl")))
    var rdd = sc.loadFromMongoDB(readConfig)
    val pipeLine = ListBuffer[Document]()
    val s1 = queries.replaceAll("\\s", "")
    var i = 0
    var j = 0
    var temp = 0


    while (i < s1.length()) {
      if (s1(i) == '{') {
        temp += 1
      }
      if (s1(i) == '}') {
        temp -= 1
      }
      // println(i,s1(i),temp)
      if (temp == 0) {
        i = i + 1
        pipeLine += Document.parse(s1.substring(j, i))
        println(s1.substring(j, i))
        j = i + 1
      }
      i = i + 1
    }


    val aggRdd = rdd.withPipeline(pipeLine)
    // rdd.
    //rdd.toDF().show()
    return aggRdd
  }

//  def parser(cmd: String) {
//    var cmd2 = cmd
//    println(cmd2)
//    val cmdParsed = JSONParser.
//
//    cmdParsed match {
//      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
//      case Some(map: Any) => {
//        var arr = map.asInstanceOf[JSONArray]
//        val s = arr.list.map {
//          x =>
//            val y = x.asInstanceOf[JSONObject]
//            println(y.toString() + "hi")
//            Document.parse(y.toString())
//        }
//
//      }
//      case None => println("Parsing failed")
//      case other => println("Unknown data structure: " + other)
//    }
//  }
}

object MongoRead {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Mongodb_read").setMaster("local")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    var s = "db.test_1.aggregate([{ $match: { $or: [ { SaleNo: { $gt: 20, $lt: 40 } },{Name: { $eq: \"cong1\" } } ] } },{ $group:{_id:{Department:\"$Department\",ID:\"$ID\"},      TotalSaleNo:{$sum:\"$SaleNo\"},Count:{$sum:1}}},    { $project:{\"_id.ID\":1,\"_id.Department\":1,Count:1,TotalSaleNo:1}}])"

    s = s.replaceAll("\\s", "")

//    check command start with db.

    s = s.drop(3)

    val collection = s.substring(0, s.indexOf('.'))
    // println(collection)
    s = s.drop(s.indexOf('.') + 1)

    if (s.substring(0, 9) == "aggregate") {
      s = s.drop(9)
    }
    s = s.dropRight(2)
    s = s.drop(2)
    //    println(s)

    val mongoRead = new MongoRead()
//    mongoRead.parser(s)

    //    for (a <- args) println(a)
    //    val queries = args.toList.drop(2)

    val rows = mongoRead.readDB(sc, "admin", collection, s).toDF().show()
    //    val queries = new ListBuffer[String]()
    //    queries += "{ $match: { $or: [ { SaleNo: { $gt: 20, $lt: 40 } },{Name: { $eq: \"cong1\" } } ] } }"
    //    queries += "{$group:{_id:{Department:\"$Department\",ID:\"$ID\"},TotalSaleNo:{$sum:\"$SaleNo\"},Count:{$sum:1}}}"
    //    queries+= "{$project:{\"_id.ID\":1,\"_id.Department\":1,Count:1,TotalSaleNo:1}}"
    //
    //    val rows = mongoRead.readDB(sc, "admin", "test_1", queries)
    //    rows.toDF().show(100)

  }
}