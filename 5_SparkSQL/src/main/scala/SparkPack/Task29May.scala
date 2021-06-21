package SparkPack
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Task29May {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkSQL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.master("local").getOrCreate()

    val csvFile = spark.read.format("csv").option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/usdata.csv")
    csvFile.show(2, false)
    csvFile.printSchema()

    val csvFile2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/Users/sanjay.a/Downloads/usdata.csv")
    csvFile2.show(2, false)
    csvFile2.printSchema()

    csvFile2.write.format("com.databricks.spark.avro").option("header", "true").mode("error").save("file:///C:/Data/avrotaks2")
    println("===============avro written====================")

  }
}