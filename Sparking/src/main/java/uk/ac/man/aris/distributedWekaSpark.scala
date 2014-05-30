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
      ///Input Parameters . ToDo: accept params as args(0), args(1) etc from command line , 
      val master="local[4]"
      val hdfsPath="hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv"
      val numberOfPartitions=4
      val numberOfAttributes=12
      val classifierToTrain="weka.classifiers.bayes.NaiveBayes"
      val metaL="default"  //default is weka.classifiers.meta.Vote
      val classAtt=11
      val randomChunks=4
      val names=new ArrayList[String]
      //Configuration of Context
      val conf=new SparkConf().setAppName("distributedWekaSpark").setMaster(master).set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      
       
      //Load Dataset and cache. ToDo: global caching strategy   -data.persist(StorageLevel.MEMORY_AND_DISK)
       var dataset=sc.textFile(hdfsPath,numberOfPartitions)
       dataset.cache()
       
       //headers
       val headerjob=new CSVToArffHeaderSparkJob
       val headers=headerjob.buildHeaders(names,numberOfAttributes,dataset)
      
       //randomize if necessary 
       //if(randomChunks>0){dataset=new WekaRandomizedChunksSparkJob().randomize(dataset, randomChunks, headers, classAtt)}
     
     
      //build a classifier+ evaluate
      val classifierjob=new WekaClassifierSparkJob
      val classifier=classifierjob.buildClassifier(metaL,classifierToTrain,classAtt,headers,dataset) 
      val evaluationJob=new WekaClassifierEvaluationSparkJob
      val eval=evaluationJob.evaluateClassifier(classifier, headers, dataset)

      //display results
      println(classifier.toString())
      evaluationJob.displayEval(eval)
      
   }
   
 
}