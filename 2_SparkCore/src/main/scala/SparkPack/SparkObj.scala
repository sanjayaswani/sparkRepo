package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._


object SparkObj {


	def main(arg:Array[String]): Unit = {

		
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val data_str= List("State->Andhra~City->Vijayawada","State->TamilNadu~City->Chennai","State->Maharashtra~City->Mumbai")  
					
					val state = data_str.flatMap(x=>x.split(",")).flatMap(x=>x.split("~")).filter(x=>x.contains("State")).map(x=>x.replace("State->",""))
					println
					println("======================State List=======================")
					state.foreach(println)
					
					val city = data_str.flatMap(x=>x.split(",")).flatMap(x=>x.split("~")).filter(x=>x.contains("City")).map(x=>x.replace("City->",""))
					println
					println("======================City List=======================")
					city.foreach(println)
					
					
	}
}