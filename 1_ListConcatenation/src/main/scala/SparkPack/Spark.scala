package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Spark {
  
  def main(arg: Array[String]): Unit={
    
    val conf = new SparkConf().setAppName("LC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val int_list=List(1,2,3)
    val int_list1=List(4,5,6)
    
    val List_Final = int_list ++ int_list1
    
    List_Final.foreach(println)
    
  }
  
}