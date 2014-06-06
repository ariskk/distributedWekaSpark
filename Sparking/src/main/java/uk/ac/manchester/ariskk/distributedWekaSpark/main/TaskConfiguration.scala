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
       case ""=>    
     }
       
    def buildHeaders():Instances={
      val headerjob=new CSVToArffHeaderSparkJob
     // headerjob.buildHeaders(options, names, numOfAttributes, data)
      return null
    }
    
    def buildClassifier():Classifier={
      val headers=buildHeaders
      val classifierjob=new WekaClassifierSparkJob
      //classifierjob.buildClassifier(metaLearner, classifierToTrain, classIndex, headers, dataset, parserOptions, classifierOptions)
      return null
    }
    
    def buildClassifierEvaluation():Evaluation={
      val headers=buildHeaders
      val classfier=buildClassifier
      val evaluationJob=new WekaClassifierEvaluationSparkJob
      
      return null
    }
    
    def buildFoldBasedClassifier():Classifier={
      val headers=buildHeaders
      val foldjob=new WekaClassifierFoldBasedSparkJob
      
      return null
    }
    
    def buildFoldBasedClassifierEvaluation():Evaluation={
      val headers=buildHeaders
      val classifier=buildFoldBasedClassifier
      val evalfoldjob=new WekaClassifierEvaluationSparkJob
      
      return null
    }
    
    def buildClusterer():Clusterer={
      val headers=buildHeaders //??
      val clustereJob=new WekaClustererSparkJob
      return null
    }
    
    def findAssociationRules():AssociationRules={
      val headers=buildHeaders
      val associationRulesJob=new WekaAssociationRulesSparkJob
      
      return null
    }
    
  

}