package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object RDDPractice1 {

case class columns(category:String,product:String,city:String,state:String,spendby:String) 


def main(Args:Array[String]):Unit={


		println("Hello world")

		val conf = new SparkConf().setAppName("First").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")


		println
		println("==============raw data print===============")
		println
		val txngym = sc.textFile("file:///C:/data/txns_gym.txt")

		txngym.foreach(println)

		println
		println("==============gym filter print===============")
		println


		val gymdata = txngym.filter(x=>x.contains("Gymnastics"))

		gymdata.foreach(println)


		println
		println("=================map split=================")
		println

		val mapsplit = gymdata.map(x=>x.split(","))

		mapsplit.foreach(println)


		println
		println("=================map split=================")
		println



		println
		println("=================Col split=================")
		println


		val colrdd = mapsplit.map(x=>columns(x(0),x(1),x(2),x(3),x(4))) // schema rdd   

		println
		println("=================filter split=================")
		println


		val fildata = colrdd.filter(x=>x.product.contains("Gymnastics"))


		fildata.foreach(println)
}}
