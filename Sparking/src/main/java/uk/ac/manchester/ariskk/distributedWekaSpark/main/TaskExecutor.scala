package uk.ac.manchester.ariskk.distributedWekaSpark.main

import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierEvaluationSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierFoldBasedSparkJob
import weka.core.Instances
import weka.classifiers.Classifier
import weka.classifiers.evaluation.Evaluation
import weka.clusterers.Clusterer
import uk.ac.manchester.ariskk.distributedWekaSpark.clusterers.WekaClustererSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.associationRules.WekaAssociationRulesSparkJob
import weka.associations.AssociationRules
import scala.collection.mutable.HashMap
import uk.ac.manchester.ariskk.distributedWekaSpark.associationRules.UpdatableRule
import weka.distributed.DistributedWekaException
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs.WekaInstancesRDDBuilder
import uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs.WekaInstanceArrayRDDBuilder
import weka.core.Instance


/**Task Configuration and submission class
 * 
 * Given a String representing the user requested task and an OptionsParser
 * it configures and initialized the execution of the task
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 * */
class TaskExecutor (sc:SparkContext, options:OptionsParser, dataset:RDD[String]) extends java.io.Serializable{
  
     val utils=new wekaSparkUtils
     val hdfsHandler=new HDFSHandler(sc)
     var dataArrayInstance:RDD[Array[Instance]]=null
     var dataInstances:RDD[Instances]=null
     var headers:Instances=null
     //caching here or in main? maybe rename??
     
     
     options.getTask match  {
       case "buildHeaders" => buildHeaders
       case "buildClassifier" => buildClassifier
       case "buildClassifierEvaluation"=> buildClassifierEvaluation
       case "buildFoldBasedClassifier"=> buildFoldBasedClassifier
       case "buildFoldBasedClassifierEvaluation"=> buildFoldBasedClassifierEvaluation
       case "buildClusterer"=> buildClusterer
       case "buildClustererEvaluation"=>buildClustererEvaluation
       case "findAssociationRules"=> findAssociationRules
       case _ => throw new DistributedWekaException("Unknown Task Identifier!")  
     }
       
    def buildHeaders():Instances={
      if(options.getHdfsHeadersInputPath==""){
      val headerjob=new CSVToArffHeaderSparkJob
      headers=headerjob.buildHeaders(options.getParserOptions,utils.getNamesFromString(options.getNames), options.getNumberOfAttributes, dataset)
      println(headers)
      hdfsHandler.saveObjectToHDFS(headers, options.getHdfsOutputPath, null)}
      else{
      headers=utils.convertDeserializedObjectToInstances(hdfsHandler.loadObjectFromHDFS(options.getHdfsHeadersInputPath))
      }
      return headers
    }
    
    def buildClassifier():Classifier={
      var classifier:Classifier=null
      if(options.getHdfsClassifierInputPath==""){
      val headers=buildHeaders
      buildRDD
      headers.setClassIndex(options.getClassIndex)
      val classifierjob=new WekaClassifierSparkJob
      options.getDatasetType match{
        case "ArrayInstance" => println("ArrayInstance");classifier=classifierjob.buildClassifier(dataArrayInstance,options.getMetaLearner, options.getClassifier, headers,  options.getParserOptions, options.getWekaOptions)
        case "Instances"     => println("Instances");classifier=classifierjob.buildClassifier(dataInstances,options.getMetaLearner, options.getClassifier, headers,  options.getParserOptions, options.getWekaOptions) 
        case "ArrayString"  => println("ArrayString");classifier=classifierjob.buildClassifier(dataset,options.getMetaLearner, options.getClassifier, headers,options.getParserOptions, options.getWekaOptions)
      }
      println(classifier)
      hdfsHandler.saveObjectToHDFS(classifier, options.getHdfsOutputPath, null)
      }
      else{
      //++ for exception handling
      classifier=utils.convertDeserializedObjectToClassifier(hdfsHandler.loadObjectFromHDFS(options.getHdfsClassifierInputPath))
      
      }
      return classifier
    }
    
    def buildClassifierEvaluation():Evaluation={
      var evaluation:Evaluation=null
      val headers=buildHeaders
      headers.setClassIndex(options.getClassIndex)
      if(options.getHdfsClassifierInputPath!=""){buildRDD}
      val classifier=buildClassifier
      val evaluationJob=new WekaClassifierEvaluationSparkJob
      options.getDatasetType match{
        case "ArrayInstance" => evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataArrayInstance, options.getClassIndex) 
        case "Instances"     => evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataInstances, options.getClassIndex) 
        case "ArrayString"   => evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex) 
      }
      //evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex)
      evaluationJob.displayEval(evaluation)
      hdfsHandler.saveObjectToHDFS(evaluation, options.getHdfsOutputPath, null)
      return evaluation
    }
    
    def buildFoldBasedClassifier():Classifier={
      var classifier:Classifier=null
      if(options.getHdfsClassifierInputPath==""){
      val headers=buildHeaders
      buildRDD
      headers.setClassIndex(options.getClassIndex)
      val foldJob=new WekaClassifierFoldBasedSparkJob
       options.getDatasetType match{
        case "ArrayInstance" => classifier=foldJob.buildFoldBasedModel(dataArrayInstance,headers, options.getNumFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
        case "Instances"     => classifier=foldJob.buildFoldBasedModel(dataInstances,headers, options.getNumFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
        case "ArrayString"   => classifier=foldJob.buildFoldBasedModel(dataset,headers, options.getNumFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
      }
      //classifier=foldJob.buildFoldBasedModel(dataset, headers, options.getNumFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
      println(classifier)
      hdfsHandler.saveObjectToHDFS(classifier, options.getHdfsOutputPath, null)
      }
      else{
      classifier=utils.convertDeserializedObjectToClassifier(hdfsHandler.loadObjectFromHDFS(options.getHdfsClassifierInputPath)) 
      }
      return classifier
    }
    
    def buildFoldBasedClassifierEvaluation():Evaluation={
      var evaluation:Evaluation=null
      val headers=buildHeaders
      headers.setClassIndex(options.getClassIndex)
      if(options.getHdfsClassifierInputPath!=""){buildRDD}
      val classifier=buildFoldBasedClassifier
      val evalFoldJob=new WekaClassifierEvaluationSparkJob
      options.getDatasetType match{
        case "ArrayInstance" => evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataArrayInstance, options.getClassIndex) 
        case "Instances"     => evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataInstances, options.getClassIndex) 
        case "ArrayString"              => evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex) 
      }
      //evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex)
      evalFoldJob.displayEval(evaluation)  
      println(evaluation)
      hdfsHandler.saveObjectToHDFS(evaluation, options.getHdfsOutputPath, null)
      return evaluation
    }
    
    def buildClusterer():Clusterer={
      var clusterer:Clusterer=null
      val headers=buildHeaders
      buildRDD
      val clustererJob=new WekaClustererSparkJob
      options.getDatasetType match {
        case "ArrayInstance" => clusterer=clustererJob.buildClusterer(dataArrayInstance, headers, "Canopy", null )
        case "Instances"     => clusterer=clustererJob.buildClusterer(dataInstances, headers, "Canopy", null )
        case "ArrayString"      => clusterer=clustererJob.buildClusterer(dataset,headers, "Canopy", null )
      }
      
      
      println(clusterer)
      hdfsHandler.saveObjectToHDFS(clusterer, options.getHdfsOutputPath, null)
      return clusterer
    }
    
    def buildClustererEvaluation():Evaluation={
      return null
    }
    
    def findAssociationRules():HashMap[String,UpdatableRule]={
      var rules:HashMap[String,UpdatableRule]=null
      val headers=buildHeaders
      buildRDD
      val associationRulesJob=new WekaAssociationRulesSparkJob
      options.getDatasetType match {
        case "ArrayInstance" => rules=associationRulesJob.findAssociationRules(dataArrayInstance,headers,  0, 0, 0)
        case "Instances"     => rules=associationRulesJob.findAssociationRules(dataInstances,headers,  0, 0, 0)
        case  "ArrayString"  => rules=associationRulesJob.findAssociationRules(dataset,headers,  0, 0, 0)
      }
      
     
     // associationRulesJob.findAssociationRules(headers, dataset, minSupport, minConfidence, minLift)
      rules.foreach{rule=> println(rule)} //++must sort and output in levels etc
      hdfsHandler.saveObjectToHDFS(rules, options.getHdfsOutputPath, null)
      associationRulesJob.displayRules(rules)
      return rules
    }
    
    def buildRDD():Unit={
      options.getDatasetType match {
        case "ArrayInstance" => dataArrayInstance=dataset.glom.map(new WekaInstanceArrayRDDBuilder().map(_,headers)); dataset.unpersist()
        case "Instances" => dataInstances=dataset.glom.map(new WekaInstancesRDDBuilder().map(_,headers)); dataset.unpersist()
        case "ArrayString" => println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      }
    }
    

}