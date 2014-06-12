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


/**Task Configuration and submission class
 * 
 * Given a String representing the user requested task and an OptionsParser
 * it configures and initialized the execution of the task
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 * */
class TaskConfiguration (task:String,options:OptionsParser){
  
  
     task match  {
       case "buildHeaders" => buildHeaders
       case "buildClassifier" => buildClassifier
       case "buildClassifierEvaluation"=> buildClassifierEvaluation
       case "buildFoldBasedClassifier"=> buildFoldBasedClassifier
       case "buildFoldBasedClassifierEvaluation"=> buildFoldBasedClassifierEvaluation
       case "buildClusterer"=> buildClusterer
       case "findAssociationRules"=> findAssociationRules
       case _ => throw new DistributedWekaException("Unknown Task Identifier!")  
     }
       
    def buildHeaders():Instances={
      val headerjob=new CSVToArffHeaderSparkJob
      val headers=headerjob.buildHeaders(null, null, options.getNumberOfAttributes, null)
      return headers
    }
    
    def buildClassifier():Classifier={
      val headers=buildHeaders
      val classifierjob=new WekaClassifierSparkJob
      val classifier=classifierjob.buildClassifier(null, null, 0, headers, null, null, null)
      return classifier
    }
    
    def buildClassifierEvaluation():Evaluation={
      val headers=buildHeaders
      val classifier=buildClassifier
      val evaluationJob=new WekaClassifierEvaluationSparkJob
      val evaluation=evaluationJob.evaluateClassifier(classifier, headers, null, 0)
      return evaluation
    }
    
    def buildFoldBasedClassifier():Classifier={
      val headers=buildHeaders
      val foldJob=new WekaClassifierFoldBasedSparkJob
      val classifier=foldJob.buildFoldBasedModel(null, null, 0, null, null, 0)
      return classifier
    }
    
    def buildFoldBasedClassifierEvaluation():Evaluation={
      val headers=buildHeaders
      val classifier=buildFoldBasedClassifier
      val evalFoldJob=new WekaClassifierEvaluationSparkJob
      val evaluation=evalFoldJob.evaluateClassifier(classifier, headers, null, 0)
      return null
    }
    
    def buildClusterer():Clusterer={
      val headers=buildHeaders //??
      val clustererJob=new WekaClustererSparkJob
      val clusterer=clustererJob.buildClusterer(headers, null, null, null)
      return clusterer
    }
    
    def findAssociationRules():HashMap[String,UpdatableRule]={
      val headers=buildHeaders
      val associationRulesJob=new WekaAssociationRulesSparkJob
      val rules=associationRulesJob.findAssociationRules(headers, null, 0, 0, 0)
      return rules
    }
    
  

}