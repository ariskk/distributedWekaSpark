package uk.ac.man.aris

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel


object SimpleSparkApp {
   def main(args : Array[String]){
      ///Config
      val conf=new SparkConf().setAppName("SimpleSparkApp").setMaster("local[4]").set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      
      //dataloading
      val data=sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/weka/recordl.csv")
      
      data.persist(StorageLevel.MEMORY_AND_DISK)
      println(data.first+"  "+data.count) 
      val data2=sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/weka/recordl.csv")
      data2.persist(StorageLevel.MEMORY_AND_DISK)
      val data3=sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/weka/recordl.csv")
      data3.persist(StorageLevel.MEMORY_AND_DISK)
      println("so far so good")
      
      println(data2.first+"  "+data2.count)
      println(data3.count)
      //val token2=data.map(line => mapp(line)).reduce(reducee(_,_))
      //println(token2)
      
    
     
      
     
   }

   def mapp (x:String) : Double ={
     var sum=0.0
     val x2=x.split(",")
     for (y <- x2){
        y.stripPrefix(",").stripSuffix(",").trim
        sum=y.toDouble/10
     }
     return sum;
   }
   def reducee (x:Double,y:Double): Double ={     
     return x+y
   }
}