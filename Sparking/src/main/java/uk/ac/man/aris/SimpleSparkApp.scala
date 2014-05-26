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
      val data=sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv",4)
     
      
      //caching
      //data.persist(StorageLevel.MEMORY_AND_DISK)
       data.cache()
     
      //headers
     var names=new ArrayList[String]
     for (i <- 1 to 12){
       names.add("att"+i)
     }
       data.glom.map(new CSVToArffHeaderSparkMapper(null).mapf(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
       println(data.collect)
      // data.mapPartitions(new CSVToArffHeaderSparkMapper2().map(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
      //data.foreachPartition(new CSVToArffHeaderSparkMapper(null).map1(_,names))
       //println(data)
       //data.mapPartitions(new CSVToArffHeaderSparkMapper(null).map2(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
      // println(data.collect)
       // data.flatMap(new CSVToArffHeaderSparkMapper(null).map4(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
     
       // println(data.collect)
     
        //val headers=data.map(line => new CSVToArffHeaderSparkMapper(null).map(line,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
      //  println(headers.toString())
        while(true){}
       
   }
   
 
}