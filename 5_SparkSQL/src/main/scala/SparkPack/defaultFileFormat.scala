package SparkPack
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object defaultFileFormat {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.master("local").getOrCreate()

    //    val csvFile = spark.read.option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/usdata.csv")
    //    csvFile.show(2, false)

    //    val jsonFile = spark.read.option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/devices.json")
    //    jsonFile.show(2, false)

//    val orcFile = spark.read.option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/part_orc.orc")
//    orcFile.show(2, false)

        val parquetFile = spark.read.option("header", "true").load("file:///C:/Users/sanjay.a/Downloads/part_par.parquet")
        parquetFile.show(2, false)

  }
}