package SparkPack

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object tnxsStruct {

  def main(args: Array[String]): Unit = {

    //    val conf = new SparkConf().setMaster("local[*]").setAppName("txns")
    //    val sc = new SparkContext(conf)
    //    sc.setLogLevel("error")

    val spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val schema_struct = StructType(Array(
      StructField("txnno", StringType, true),
      StructField("txndate", StringType, true),
      StructField("custno", StringType, true),
      StructField("amount", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("spendby", StringType, true)))

    val txnsFile = spark.read.schema(schema_struct).format("csv").load("file:///C:/Users/sanjay.a/Downloads/txns")
    txnsFile.show()

  }
}