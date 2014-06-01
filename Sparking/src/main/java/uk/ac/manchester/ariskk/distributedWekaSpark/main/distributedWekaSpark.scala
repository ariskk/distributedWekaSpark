package uk.ac.manchester.ariskk.distributedWekaSpark.main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import weka.core.Utils
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierEvaluationSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierFoldBasedSparkJob


/** Project main class 
 *  
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   
 *   ToDo: user-interface, option parser and loader-saver for persistence  */


object distributedWekaSpark {
   def main(args : Array[String]){
      ///Input Parameters . ToDo: accept params as args(0), args(1) etc from command line , 
      val master="local[4]"
      val hdfsPath="hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv"
      val numberOfPartitions=4
      val numberOfAttributes=12
      val classifierToTrain="weka.classifiers.bayes.NaiveBayes"
      val metaL="weka.classifiers.meta.Bagging"  //default is weka.classifiers.meta.Vote
      val classAtt=11
      val randomChunks=4
      val names=new ArrayList[String]
      val folds=3
      
      
      // Option parsing: should be a class
      val options="-W weka.classifiers.tress.J48  -seed 2L -num-nodes 3"
      val split=Utils.splitOptions(options)
      val optA=Utils.getOption("num-nodes", split)
      println(optA)
      //
      
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
       if(randomChunks>0){dataset=new WekaRandomizedChunksSparkJob().randomize(dataset, randomChunks, headers, classAtt)}
       
     //build foldbased
      val foldjob=new WekaClassifierFoldBasedSparkJob
      val classifier=foldjob.buildFoldBasedModel(dataset, headers, folds, classifierToTrain, metaL)
      println(classifier.toString())
      val evalfoldjob=new WekaClassifierEvaluationSparkJob
      val eval=evalfoldjob.evaluateFoldBasedClassifier(folds, classifier, headers, dataset)
      evalfoldjob.displayEval(eval)
      
      //build a classifier+ evaluate
      val classifierjob=new WekaClassifierSparkJob
      val classifier2=classifierjob.buildClassifier(metaL,classifierToTrain,classAtt,headers,dataset) 
      val evaluationJob=new WekaClassifierEvaluationSparkJob
      val eval2=evaluationJob.evaluateClassifier(classifier, headers, dataset)

      println(classifier.toString())
      evaluationJob.displayEval(eval)
      
   }
   
     
}