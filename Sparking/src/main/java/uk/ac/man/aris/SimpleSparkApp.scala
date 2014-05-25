package uk.ac.man.aris

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import weka.classifiers.bayes.net.search.fixed.NaiveBayes
import weka.distributed.WekaClassifierMapTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import java.util.ArrayList



object SimpleSparkApp {
   def main(args : Array[String]){
      ///Config
      val conf=new SparkConf().setAppName("SimpleSparkApp").setMaster("local[4]").set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      
      //dataloading
      val data=sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/weka/breast.csv")
      
      data.persist(StorageLevel.MEMORY_AND_DISK)
    
     
     var names=new ArrayList[String]
     for (i <- 1 to 10){
       names.add("att"+i)
     }
      println(data.first)
       val mapper=new CSVToArffHeaderSparkMapper(null)
       val reducer=new CSVToArffHeaderSparkReducer
       val headers=data.map(line => mapper.map(line.toString(),names)).reduce(reducer.reduce(_,_))
       println(headers.toString())
   }
   
    
   
   
   
   
   
   
   
   
   
   
   
   def mapp2(x:String) :Double ={
     if (x.contains("?")){return 1}
     else {return 0;}
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