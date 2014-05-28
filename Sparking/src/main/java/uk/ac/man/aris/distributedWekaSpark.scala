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



object distributedWekaSpark {
   def main(args : Array[String]){
    ///Input Parameters . to-do: accept args(0), args(1) from command line , accept task details
      val master="local[4]"
      val hdfsPath="hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv"
      val numberOfPartitions=4
      val numberOfAttributes=12
      
      //Configuration of Context
      val conf=new SparkConf().setAppName("distributedWekaSpark").setMaster(master).set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      
       
      //Load Dataset and cache. to-do: global caching strategy   -data.persist(StorageLevel.MEMORY_AND_DISK)
       
       val dataset=sc.textFile(hdfsPath,numberOfPartitions)
       dataset.cache()
       
    
     
     
      val headerjob=new CSVToArffHeaderSparkJob
      val header=headerjob.buildHeaders(numberOfAttributes,dataset)
      val classifierjob=new WekaClassifierSparkJob
      val classifier=classifierjob.buildClassifier(header,dataset) 
       println(classifier.toString())
   }
   
 
}