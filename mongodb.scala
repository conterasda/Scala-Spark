
import scala.collection.JavaConversions._
import org.bson.conversions
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import org.apache.log4j.{Level, Logger}
import org.apache.poi.xwpf.usermodel.Document
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.mongodb
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoDB
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.query.dsl.QueryExpressionObject
import com.mongodb.casbah.commons.MongoDBObject

class mongodb {
  def writeToMongo(sparkSession: SparkSession
                   , dataToSave: DataFrame, database: String
                   , collection: String): Unit = {
    MongoSpark.save(dataToSave.write.mode("append")
      , WriteConfig(Map("spark.mongodb.output.uri"
        -> ("mongodb://127.0.0.1/" + database + "." + collection))))

    //  MongoSpark.save(dataToSave.write.option(database, collection).mode("append"))
    println("Luu data thanh cong vao: " + database + "." + collection)
  }

  def readFromMongo(sparkSession: SparkSession
                    , database: String, collection: String): DataFrame = {
    val dataframe = sparkSession
      .read.format("com.mongodb.spark.sql.DefaultSource")
      .option("spark.mongodb.input.uri"
        , "mongodb://127.0.0.1/" + database + "." + collection)
      .load()

    return dataframe
  }

  def findMongo(query: DBObject, fields: DBObject, database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val mongoCollect = mongoClient(database)(collection)
    val results = mongoCollect.find(query, fields)
    for (result <- results) {
      println(result)
    }
    return results
  }


  def deleteData(query: DBObject, database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val mongoCollect = mongoClient(database)(collection)

    print(mongoCollect.findAndRemove(query))

  }

  def deleteCollection(database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val mongoCollect: MongoCollection = mongoClient(database)(collection)
    mongoCollect.remove(MongoDBObject.newBuilder.result)
    println("Xoa data thanh cong tai: " + database + "." + collection)
  }

  def insertMongo(insertData: DBObject, database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val mongoCollect = mongoClient(database)(collection)
    mongoCollect += insertData
  }

  def updateMongo(query: DBObject, updateData: DBObject, multi: Boolean, database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val mongoCollect = mongoClient(database)(collection)
    val update = MongoDBObject("$set" -> updateData)
    println(mongoCollect.update(query, update, false, multi))

  }

  def sumMongo(sum: String, groupby: String, database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val mongoDatabase = mongoClient(database)
    val mongoCollection = mongoDatabase(collection)
    val group = "$" + groupby
    val s = "$" + sum

    val aggregationOptions = AggregationOptions(AggregationOptions.CURSOR)

    val results = mongoCollection.aggregate(
      List(
        MongoDBObject("$group" ->
          MongoDBObject("_id" -> MongoDBObject(groupby -> group),
            "Total" + sum -> MongoDBObject("$sum" -> s))
        )
      )
      , aggregationOptions
    )
    for (result <- results) {
      println(result)
    }

    return results
  }


  def sumMongoByStr(sum: String, groupby: String, database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val mongoDatabase = mongoClient(database)
    val mongoCollection = mongoDatabase(collection)
    val group = "$" + groupby
    val s = "$" + sum
    val aggregationOptions = AggregationOptions(AggregationOptions.CURSOR)
    val results = mongoCollection.aggregate(
      List(
        MongoDBObject("$group" ->
          MongoDBObject("_id" ->
            MongoDBObject("Month" -> MongoDBObject("$substr" -> (group, 4, 2))),
            "Total" + sum -> MongoDBObject("$sum" -> s),
            "count" -> MongoDBObject("$sum" -> 1)
          )
        ),
        MongoDBObject("$sort" -> MongoDBObject("count" -> (1)))

      )

      , aggregationOptions
    )
    for (result <- results) {
      println(result)
    }

    return results
  }

  def q(query: String, database: String, collection: String): Unit = {
    val mongoClient: MongoClient = MongoClient("localhost", 27017)
    val db = mongoClient(database) //(collection)
    println(query)
    println(db.command("test_1.find({})")
      .get("result"))
  }

}

object mongodb {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Mongodb_read").setMaster("local")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val schema = StructType(Seq(
      StructField("ID", IntegerType, nullable = false),
      StructField("Name", StringType, nullable = true),
      StructField("Department", StringType, nullable = true),
      StructField("SaleNo", IntegerType, nullable = true),
      StructField("Date", StringType, nullable = true)
    ))

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      //      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/admin.test_1")
      //      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/admin.test_1")
      //  .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
      //  .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "1")
      .getOrCreate()
    //    import sparkSession.implicits._

    val df = sparkSession.read.option("header", "false")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", false)
      .schema(schema)
      .csv("/home/admin/IdeaProjects/project1/file.txt")


    val mongo = new mongodb()


    // 1 to 100 foreach { _ => mongo.writeToMongo(sparkSession, df,"admin","test_1")}
    // val df1 = mongo.readFromMongo(sparkSession,"admin","test_1").show(30)
    val update = MongoDBObject("Name" -> "lu")
    val query = MongoDBObject("ID" -> 7)
    val field = MongoDBObject("ID" -> 1, "Name" -> 1, "SaleNo" -> 1, "_id" -> 0)
//    mongo.updateMongo(MongoDBObject("ID" -> MongoDBObject("$lt" -> 2)), update, true, "admin", "test_1")
    mongo.updateMongo($and("ID" $eq 2,"SaleNo" $gt 30),update,true, "admin", "test_1")
    //    val insert = MongoDBObject("ID" -> 7,
    //      "Name" -> "trcong", "Department" -> "abc",
    //      "SaleNo" -> 99, "Date" -> "20170209", "_id" -> 0)
    //   mongo.deleteData("ID" $eq 7,"admin","test_1")
    //mongo.insertMongo(insert,"admin","test_1")

    // mongo.findMongo("ID" $ne 1,field,"admin","test_1")
    //  mongo.deleteCollection("admin","test_1")


    //val result = mongo.sumMongoByStr("SaleNo","Date","admin","test_1")
    //result.toDF()
    //mongo.findMongo()

    //    mongo.findMongo(MongoDBObject("SaleNo"->MongoDBObject( "$gte"-> 10,"$lt"->45)
    //      ,"ID"->MongoDBObject( "$gt"->3))
    //      ,field, "admin","test_1")

    //  mongo.findMongo(MongoDBObject("$expr"->MongoDBObject("$gt"->("SaleNo","ID"))),field, "admin","test_1")

    //mongo.q(".find({})", "admin", "test_1")
    //  mongo.readQuery(query,"admin","test_1")

    //    val df1 = MongoSpark.load(sparkSession)
    //      .show(30)
    sparkSession.stop()
  }
}