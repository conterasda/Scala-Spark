
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class read_sql {

  def readFromSQL(sparkSession: SparkSession
                  , database: String, table: String
                  , username: String, password: String): DataFrame = {
    val prop = new Properties()
    prop.setProperty("user", username)
    prop.setProperty("password", password)
    val dataFrame = sparkSession.read
      .jdbc(s"jdbc:mysql://localhost:3306/" + database, table, prop)
    return dataFrame
  }

  def writeToSQL(sparkSession: SparkSession, dataToWrite: DataFrame
                 , database: String, table: String
                 , username: String, password: String): Unit = {
    val prop = new Properties()
    prop.setProperty("user", username)
    prop.setProperty("password", password)
    dataToWrite.write.jdbc(s"jdbc:mysql://localhost:3306/" + database, table, prop)
  }
}

object read_sql {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLread").setMaster("local")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    import org.apache.spark.sql.SparkSession
    val sparkSess = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()





    //    val prop = new Properties()
    //    prop.setProperty("user", "root")
    //    prop.setProperty("password", "chien@37cong")
    //    df.write.jdbc(s"jdbc:mysql://localhost:3306/test", "my_new_table", prop)


    //    val df1 = sparkSess.read
    //      .jdbc(s"jdbc:mysql://localhost:3306/test", "my_new_table", prop)
    //      .show(30)
    val readSQL = new read_sql()

    //    val schema = StructType(Seq(
    //      StructField("ID",IntegerType,nullable = false),
    //      StructField("Name",StringType,nullable = true),
    //      StructField("Department",StringType,nullable = true),
    //      StructField("SaleNo",IntegerType,nullable = true),
    //      StructField("Date",StringType,nullable = true)
    //    ))
    //    val df = sparkSess.read.option("header","false")
    //      .option("ignoreLeadingWhiteSpace",true)
    //      .option("ignoreTrailingWhiteSpace",false)
    //      .schema(schema)
    //      .csv("/home/admin/IdeaProjects/project1/file.txt")
    //    readSQL.writeToSQL(sparkSess,df,"test","my_new_table","root","chien@37cong")

    val dataFrame = readSQL
      .readFromSQL(sparkSess, "test", "my_new_table", "root", "chien@37cong")
    dataFrame.show(30)
    sparkSess.stop()
  }
}