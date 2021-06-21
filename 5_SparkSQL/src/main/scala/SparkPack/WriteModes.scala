package SparkPack
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WriteModes {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.master("local").getOrCreate()

    val csvFile = spark.read.format("csv").option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/usdata.csv")
    csvFile.show(2, false)

    csvFile.write.format("parquet").mode("ignore").save("file:///C:/Data/output/parquet")

  }
}