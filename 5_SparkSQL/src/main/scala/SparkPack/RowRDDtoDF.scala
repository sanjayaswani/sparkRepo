package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object RowRDDtoDF {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println()
    println("===========fileImported=========================")
    val data = sc.textFile("file:///C:/ZeyoDataFiles/txns")
    data.take(10).foreach(println)

    println()
    println("===========mapSplit=========================")
    val maprdd = data.map(x => x.split(","))
    maprdd.take(10).foreach(println)

    println()
    println("===========caseclass apply=========================")
    val maprdd_split = maprdd.map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    maprdd_split.take(10).foreach(println)

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

    println()
    println("===========DF from RowRDD=========================")
    val df = spark.createDataFrame(maprdd_split, schema_struct)
    df.show()

    println()
    println("===========DF with filters=========================")
    df.createOrReplaceTempView("txndf")
    val cashdf = spark.sql("select * from txndf where product like '%Gymnastics%' and txnno	>40000")
    cashdf.show()

  }

}
