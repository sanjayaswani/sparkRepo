package SparkPack
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RdbmsSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.master("local").getOrCreate()

    val mysqlDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/zeyodb")
      .option("dbtable", "web_customer")
      .option("user", "root")
      .option("password", "Aditya908")
      .load()

    mysqlDF.show(2)
    mysqlDF.printSchema()

    mysqlDF.write.format("parquet").mode("ignore").save("file:///C:/Data/output/rdbms_spark")

  }
}