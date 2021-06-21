package SparkPack
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object xmlFileRead {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.master("local").getOrCreate()

    val xmlnoteFile = spark.read.format("com.databricks.spark.xml").option("rowTag", "note").load("file:///C:/SparkInputDataSets/note.xml")
    xmlnoteFile.show(2)
    xmlnoteFile.printSchema()

    val xmlbookFile = spark.read.format("com.databricks.spark.xml").option("rowTag", "book").load("file:///C:/SparkInputDataSets/book.xml")
    xmlbookFile.show(2)
    xmlbookFile.printSchema()
    
    val xmltranFile = spark.read.format("com.databricks.spark.xml").option("rowTag", "POSLog").load("file:///C:/SparkInputDataSets/transactions.xml")
    xmltranFile.show(2,false)
    xmltranFile.printSchema()
  }
}