/*
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 *    TaskExecutor.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.rdd.HadoopRDD


/**Task Configuration and submission class
 * 
 * Given a String representing the user requested task and an OptionsParser
 * it configures and initialized the execution of the task
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 * */
class TaskExecutor (hdfsHandler:HDFSHandler, options:OptionsParser,caching:StorageLevel) extends java.io.Serializable{
  
     val utils=new wekaSparkUtils
     val datasetType=options.getDatasetType
    
     
     var dataset=hdfsHandler.loadRDDFromHDFS(options.getHdfsDatasetInputPath, options.getNumberOfPartitions)
    
     
     
     dataset.persist(caching)
     
     var dataArrayInstance:RDD[Array[Instance]]=null
     var dataInstances:RDD[Instances]=null
     var headers:Instances=null
     
     var rddmadeflag=false
     var randomFlag=false

     //strat++
     
     
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
      val headerjob=new CSVToArffHeaderSparkJob
      
      if(options.getHdfsHeadersInputPath==""){
      var names=options.getNames
      val path=options.getNamesPath
      if(path!=""){
      names=hdfsHandler.loadRDDFromHDFS(path,1).collect.mkString("")
      }
      headers=headerjob.buildHeaders(options.getParserOptions,utils.getNamesFromString(names), options.getNumberOfAttributes, dataset,options.generateHeaderStatistics)
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
      datasetType match{
        case "ArrayInstance" => println("ArrayInstance");classifier=classifierjob.buildClassifier(dataArrayInstance,options.getMetaLearner, options.getClassifier, headers,  options.getParserOptions, options.getWekaOptions)
        case "Instances"     => println("Instances");classifier=classifierjob.buildClassifier(dataInstances,options.getMetaLearner, options.getClassifier, headers,  options.getParserOptions, options.getWekaOptions) 
        case "ArrayString"  => println("ArrayString");classifier=classifierjob.buildClassifier(dataset,options.getMetaLearner, options.getClassifier, headers,options.getParserOptions, options.getWekaOptions)
      }
      println("Done!")
      println(classifier)
      hdfsHandler.saveObjectToHDFS(classifier, options.getHdfsOutputPath, null)
      }
      else{
      classifier=utils.convertDeserializedObjectToClassifier(hdfsHandler.loadObjectFromHDFS(options.getHdfsClassifierInputPath))
      
      }
      return classifier
    }
    
    def buildClassifierEvaluation():Evaluation={
      var evaluation:Evaluation=null
      val headers=buildHeaders
      headers.setClassIndex(options.getClassIndex)
      if(options.getHdfsClassifierInputPath==""){buildRDD}
      val classifier=buildClassifier
      val evaluationJob=new WekaClassifierEvaluationSparkJob
      datasetType match{
        case "ArrayInstance" => evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataArrayInstance, options.getClassIndex,options.getParserOptions) 
        case "Instances"     => evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataInstances, options.getClassIndex,options.getParserOptions) 
        case "ArrayString"   => evaluation=evaluationJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex,options.getParserOptions) 
      }
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
       datasetType match{
        case "ArrayInstance" => classifier=foldJob.buildFoldBasedModel(dataArrayInstance,headers, options.getNumberOfFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
        case "Instances"     => classifier=foldJob.buildFoldBasedModel(dataInstances,headers, options.getNumberOfFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
        case "ArrayString"   => classifier=foldJob.buildFoldBasedModel(dataset,headers, options.getNumberOfFolds, options.getClassifier, options.getMetaLearner, options.getClassIndex)
      }
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
      datasetType match{
        case "ArrayInstance" => evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataArrayInstance, options.getClassIndex,options.getParserOptions) 
        case "Instances"     => evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataInstances, options.getClassIndex,options.getParserOptions) 
        case "ArrayString"   => evaluation=evalFoldJob.evaluateClassifier(classifier, headers, dataset, options.getClassIndex,options.getParserOptions) 
      }
      evalFoldJob.displayEval(evaluation)  
      hdfsHandler.saveObjectToHDFS(evaluation, options.getHdfsOutputPath, null)
      return evaluation
    }
    
    def buildClusterer():Clusterer={
      var clusterer:Clusterer=null
      val headers=buildHeaders
      buildRDD
      val clustererJob=new WekaClustererSparkJob
      datasetType match {
        case "ArrayInstance" => clusterer=clustererJob.buildClusterer(dataArrayInstance, headers, "Canopy", options.getWekaOptions ,options.getNumberOfClusters,options.getDistanceMetric)
        case "Instances"     => clusterer=clustererJob.buildClusterer(dataInstances, headers, "Canopy", options.getWekaOptions ,options.getNumberOfClusters,options.getDistanceMetric)
        case "ArrayString"   => clusterer=clustererJob.buildClusterer(dataset,headers, "Canopy", options.getWekaOptions ,options.getNumberOfClusters,options.getDistanceMetric)
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
      datasetType match {
        case "ArrayInstance" => rules=associationRulesJob.findAssociationRules(dataArrayInstance,headers,options.getParserOptions,options.getWekaOptions)
        case "Instances"     => rules=associationRulesJob.findAssociationRules(dataInstances,headers,options.getParserOptions,options.getWekaOptions)
        case  "ArrayString"  => rules=associationRulesJob.findAssociationRules(dataset,headers, options.getParserOptions,options.getWekaOptions)
      }
      
    
      
      hdfsHandler.saveObjectToHDFS(rules, options.getHdfsOutputPath, null)
      associationRulesJob.displayRules(rules)
      return rules
    }
    def buildRDD():Unit={
      if(rddmadeflag==false){
      datasetType match {
        case "ArrayInstance" => dataArrayInstance=dataset.glom.map(new WekaInstanceArrayRDDBuilder(headers).map(_))
        case "Instances" => dataInstances=dataset.glom.map(new WekaInstancesRDDBuilder(headers).map(_))
        case "ArrayString" => println("Using ArrayString")
      }
      rddmadeflag==true}
    }
    

}