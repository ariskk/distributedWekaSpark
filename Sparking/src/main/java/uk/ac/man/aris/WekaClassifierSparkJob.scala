package uk.ac.man.aris

import weka.classifiers.Classifier
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.ArrayList
import weka.core.Instances
import weka.core.Utils._
import weka.classifiers.Classifier._
import org.apache.spark.rdd.RDD

class WekaClassifierSparkJob extends java.io.Serializable {
          var options= new Array[String](1)
  
  def buildClassifier (headers:Instances,data:RDD[String]) : Classifier = {
     
        // var arffHeaders:Instances=headers
         //options(0)=("-W weka.classifiers.meta.Bagging")
         options(0)=("w weka.classifiers.bayes.NaiveBayes")

         val classifier=data.glom.map(new WekaClassifierSparkMapper(null,null,headers).map(_)).reduce(new WekaClassifierSparkReducer(null).reduce(_,_))
         return classifier
  }
    
      
}