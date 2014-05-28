package uk.ac.man.aris

import weka.distributed.WekaClassifierEvaluationMapTask
import weka.classifiers.Classifier
import weka.classifiers.evaluation.Evaluation

class WekaClassifierEvaluationSparkMapper {
   var m_task=new WekaClassifierEvaluationMapTask
   
   
   def map (rows:Array[String],classifier:Classifier): Evaluation={
     for(x <- rows){
       
       
     }
     
     
     return m_task.getEvaluation()
   }


}