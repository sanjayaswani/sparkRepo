package sparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CaseClassImplement {
case class txnCaseClass(txnno: String, txndate :String, custno: String, amount: String,category: String, product: String, city :String, state: String, spendby :String)

def main(arg: Array[String]):Unit= {


		val conf = new SparkConf().setAppName("SchemaRDD").setMaster("local[*]")
				val sc = new SparkContext(conf)
				sc.setLogLevel("error")


				println()
				println("===========fileImported=========================")
				val data = sc.textFile("file:///C:/ZeyoDataFiles/txns")
				data.take(10).foreach(println)


				println()
				println("===========mapSplit=========================")
				val dataMap = data.map(x=>x.split(","))
				dataMap.take(10).foreach(println)

				println()
				println("===========caseClaseApply=========================")

				val schemaData = dataMap.map(x=>txnCaseClass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				schemaData.take(10).foreach(println)


				println()
				println("===========filter=========================")
				val filteredRDD = schemaData.filter(x=>x.product.contains("Gymnastics") & x.spendby.contains("cash"))
				filteredRDD.take(10).foreach(println)

}
}