import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

class spark_read {
  def readFromText(sparkSession: SparkSession
                   , schema: StructType, fileLink: String): DataFrame = {
    val df = sparkSession.read.option("header", "false")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", false)
      .schema(schema)
      .csv(fileLink)
    return df
  }

  //  def sumColumn(dataFrame: DataFrame): DataFrame ={
  //        val sums = cols.map(colName => sum(colName).cast("Int").as("sum_" + colName))
  //        dfWithSchema.groupBy("Deparment").agg(sums.head, sums.tail:_*).show()
  //  }
}

object spark_read {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Sparkread").setMaster("local")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)
      .setLogLevel("ERROR")

    // val sqlContext = new SQLContext(sc )
    // import sqlContext.implicits._

    //    val schemaString = "ID name Department SaleNo Date"
    //    val fields = schemaString.split(" ")
    // .map(fieldName => StructField(fieldName,
    //      StringType, nullable = true))
    //    val schema = StructType(fields)

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
      .getOrCreate()


    import sparkSession.implicits._

    val spark = new spark_read()
    val df = spark.readFromText(sparkSession, schema, "/home/admin/IdeaProjects/project1/file.txt")



    //df.show(30)

    //    val cols = List("SaleNo")


    //    df.groupBy("Department").agg(sum("SaleNo")
    //      //.cast("Int")
    //      .as("sum_SaleNo"),
    //      countDistinct("ID").as("total_Saleman")).show()
    //
    //    df.groupBy("Department")
    //      .agg(max("SaleNo")
    //        //.cast("Int")
    //      .as("max_SaleNo")).show()

    //    df1.groupBy("Month").agg(sum("SaleNo")
    // .cast("Int")
    //      .as("month_SaleNo")).show()

    //    df.groupBy(substring(col("Date"),5,2).as("Month"))
    //      .agg(sum("SaleNo")
    //        .as("total_SaleNo")).show()


    //    df.show(30)
    //
    //val month_data =
    //.show()

    //    val month_data1 = month_data.map(x => ((x._5), (x._4)))
    //        .reduce((x,y)=>(x._3+y._3))
    //        .show()
    //month_data.toDF().show()
    //    val month_data1 = month_data
    //      .toDF("ID name Department SaleNo Month").rdd
    //month_data1.groupBy("Month").agg(sum("SaleNo")).show()


    val month_data1 = df.map(x =>
      (x.getInt(0), x.getString(1),
        x.getString(2), x.getInt(3),
        x.getString(4).substring(4, 6)))
      .map { case (id, name, deparment, saleNo, month) => (id, name, deparment, saleNo, month, 1) }
      .map { case (id, name, deparment, saleNo, month, value) => ((month), (saleNo, value)) }
      //.map{case (id, name, deparment, saleNo, month) => ((month),(saleNo,20))}
      .groupByKey(_._1)
      .reduceGroups((x, y) => (x._1, (x._2._1 + y._2._1, x._2._2 + y._2._2)))
      .map(_._2)
      .toDF("Month", "SaleNo")
      .show()

    //    val month_data2 = month_data1.map{case((month),saleNo)=>((month),(saleNo.sum))}


    //    val month_data2 = df.map{case (id, name, deparment, saleNo,month) => (month,saleNo)}
    //    reduceByKey((x, y) => (x._4, y._4))
    //      .collect()
    //    month_data2.toDF().show()

    //month_data1.toDF().show()
    //   df.rdd.map((3).asInstanceOf[Int]).reduce(_+).show()

    sparkSession.stop()
  }


  //  def import_file(sparkSession: SparkSession, schema: StructType): Unit ={
  //    val df = sparkSession.read.option("header","false")
  //      .option("ignoreLeadingWhiteSpace",true)
  //      .option("ignoreTrailingWhiteSpace",false)
  //      .schema(schema)
  //      .csv("/home/admin/IdeaProjects/project1/file.txt")
  //    return df
  //  }
}