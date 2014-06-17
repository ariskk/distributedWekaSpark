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


/**Task Configuration and submission class
 * 
 * Given a String representing the user requested task and an OptionsParser
 * it configures and initialized the execution of the task
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 * */
class TaskConfiguration (sc:SparkContext,task:String,options:OptionsParser,dataset:RDD[String],dstype:String){
  
     val utils=new wekaSparkUtils
     val hdfsHandler=new HDFSHandler(sc)
     var data=dataset
     //caching here or in main? maybe rename??
     options.getDatasetType match {
       case "ArrayString" =>
       case "ArrayInstance" => 
       case "Instances"=>
       case _ => println("Unrecognised or Unsupported input format, will use Array[String] instead")
       
     }
     
     task match  {
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
      var headers:Instances=null
      if(options.getHdfsHeadersInputPath==""){
      val headerjob=new CSVToArffHeaderSparkJob
      headers=headerjob.buildHeaders(options.getWekaOptions,utils.getNamesFromString(options.getNames.mkString("")), options.getNumberOfAttributes, dataset)
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
      headers.setClassIndex(options.getClassIndex)
      val classifierjob=new WekaClassifierSparkJob
      classifier=classifierjob.buildClassifier(dataset,options.getMetaLearner, options.getClassifier, headers,  null, options.getWekaOptions)
      println(classifier)
      hdfsHandler.saveObjectToHDFS(classifier, options.getHdfsOutputPath, null)
      //classifierjob.buildClassifier(metaLearner, classifierToTrain, classIndex, headers, dataset, parserOptions, classifierOptions)
      }
      else{
      classifier=utils.convertDeserializedObjectToClassifier(hdfsHandler.loadObjectFromHDFS(options.getHdfsClassifierInputPath))
      }
      return classifier
    }
    
    def buildClassifierEvaluation():Evaluation={
      val headers=buildHeaders
      headers.setClassIndex(options.getClassIndex)
      val classifier=buildClassifier
      val evaluationJob=new WekaClassifierEvaluationSparkJob
      val evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex)
       println(evaluation)
      hdfsHandler.saveObjectToHDFS(evaluation, options.getHdfsOutputPath, null)
      return evaluation
    }
    
    def buildFoldBasedClassifier():Classifier={
      var classifier:Classifier=null
      if(options.getHdfsClassifierInputPath==""){
      val headers=buildHeaders
      headers.setClassIndex(options.getClassIndex)
      val foldJob=new WekaClassifierFoldBasedSparkJob
      classifier=foldJob.buildFoldBasedModel(dataset, headers, options.getNumFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
    // foldJob.buildFoldBasedModel(dataset, headers, folds, classifierToTrain, metaLearner, classIndex)
      println(classifier)
      hdfsHandler.saveObjectToHDFS(classifier, options.getHdfsOutputPath, null)
      }
      else{
      classifier=utils.convertDeserializedObjectToClassifier(hdfsHandler.loadObjectFromHDFS(options.getHdfsClassifierInputPath)) 
      }
      return classifier
    }
    
    def buildFoldBasedClassifierEvaluation():Evaluation={
      val headers=buildHeaders
      headers.setClassIndex(options.getClassIndex)
      val classifier=buildFoldBasedClassifier
      val evalFoldJob=new WekaClassifierEvaluationSparkJob
      val evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex)
      evalFoldJob.displayEval(evaluation)  
      println(evaluation)
      hdfsHandler.saveObjectToHDFS(evaluation, options.getHdfsOutputPath, null)
      return evaluation
    }
    
    def buildClusterer():Clusterer={
      val headers=buildHeaders //??
      val clustererJob=new WekaClustererSparkJob
      val clusterer=clustererJob.buildClusterer(headers, null, null, null)
      println(clusterer)
      hdfsHandler.saveObjectToHDFS(clusterer, options.getHdfsOutputPath, null)
      return clusterer
    }
    
    def buildClustererEvaluation():Evaluation={
      return null
    }
    
    def findAssociationRules():HashMap[String,UpdatableRule]={
      val headers=buildHeaders
      val associationRulesJob=new WekaAssociationRulesSparkJob
      val rules=associationRulesJob.findAssociationRules(headers, dataset, 0, 0, 0)
     // associationRulesJob.findAssociationRules(headers, dataset, minSupport, minConfidence, minLift)
      rules.foreach{rule=> println(rule)} //++must sort and output in levels etc
      hdfsHandler.saveObjectToHDFS(rules, options.getHdfsOutputPath, null)
      return rules
    }
    
  

}