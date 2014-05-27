package uk.ac.man.aris

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import weka.classifiers.bayes.net.search.fixed.NaiveBayes
import weka.distributed.WekaClassifierMapTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import java.util.ArrayList
import weka.core.Instances



object SimpleSparkApp {
   def main(args : Array[String]){
      ///Config
      val conf=new SparkConf().setAppName("SimpleSparkApp").setMaster("local[4]").set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      
      //dataloading
      val data=sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/weka/susy1m",8)
     
      
      //caching Only if this fits in memory! else either outofmem exception or need to implement caching stategy
      //data.persist(StorageLevel.MEMORY_AND_DISK)
      // data.persist(StorageLevel.DISK_ONLY)
     
      //headers
     var names=new ArrayList[String]
     for (i <- 1 to 19){
       names.add("att"+i)
     }
      
      //compute headers
       val headers=data.glom.map(new CSVToArffHeaderSparkMapper(null).map(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
       println(headers.toString)
      
        
       
   }
   
 
}