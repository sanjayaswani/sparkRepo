package SparkPack
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PartitionBy {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.master("local").getOrCreate()

    val txnsFile = spark.read.format("csv").option("header","true").load("file:///C:/SparkInputDataSets/txnsWithHeader")
    txnsFile.show()
    
    txnsFile.write.format("parquet").mode("overwrite").partitionBy("category", "Spendby").save("file:///C:/Data/output/parquet_partition")

    print("Data Written On disk")
  }
}