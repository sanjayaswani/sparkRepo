package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SchemaRDDtoDF {

  case class columns(tnxno: String, date: String, custno: String, amount: String, category: String, product: String, city: String, state: String, spendby: String)

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
    val maprdd_split = maprdd.map(x => columns(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    maprdd_split.take(10).foreach(println)

    val df = maprdd_split.toDF()
    df.show()

    df.createOrReplaceTempView("txndf")

    val cashdf = spark.sql("select * from txndf where spendby like '%cash%'")
    cashdf.show()
    
    val DFtoRDD = cashdf.rdd //this will be always RowRDD not schemaRDD
    
    val DFtoRDD2 = DFtoRDD.map(x=>x.mkString("\\n"))
    
    DFtoRDD2.take(10).foreach(println)
    
    
    

  }

}
