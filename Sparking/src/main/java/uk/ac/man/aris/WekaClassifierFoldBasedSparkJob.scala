package uk.ac.man.aris

import weka.core.Instances
import org.apache.spark.rdd.RDD
import java.util.ArrayList
import weka.classifiers.Classifier

class WekaClassifierFoldBasedSparkJob extends java.io.Serializable{
  
  
  
  
  def buildFoldBasedModel(dataset:RDD[String],headers:Instances,folds:Int,toTrain:String,metaL:String): ArrayList[Classifier]={
    
    
    val classifiers=dataset.glom.map(new WekaClassifierFoldBasedSparkMapper(folds,headers,toTrain,metaL).map(_)).reduce(new WekaClassifierFoldBasedSparkReducer(folds).reduce(_,_))
    
   return classifiers}

}