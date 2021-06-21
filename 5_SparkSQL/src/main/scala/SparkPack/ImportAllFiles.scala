package SparkPack
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ImportAllFiles {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.master("local").getOrCreate()

    val csvFile = spark.read.format("csv").option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/usdata.csv")
    csvFile.show(2, false)

    val jsonFile = spark.read.format("json").option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/devices.json")
    jsonFile.show(2, false)

    val parquetFile = spark.read.format("parquet").option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/part_par.parquet")
    parquetFile.show(2, false)

    val orcFile = spark.read.format("orc").option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/part_orc.orc")
    orcFile.show(2, false)

    val avroFile = spark.read.format("com.databricks.spark.avro").option("header", "true").load("file:///C:/Data/avro/part-00000-c430b931-105b-4543-9aca-4a55cd150c8f-c000.avro")
    avroFile.show(2, false)

    orcFile.write.format("parquet").mode("error").save("file:///C:/Data/parquet")
    println("===============parquet written====================")

    orcFile.write.format("orc").mode("error").save("file:///C:/Data/orc")
    println("===============orc written====================")

    orcFile.write.format("csv").option("header", "true").mode("error").save("file:///C:/Data/csv")
    println("===============csv written====================")

    orcFile.write.format("com.databricks.spark.avro").mode("error").save("file:///C:/Data/avro")
    println("===============avro written====================")
  }
}