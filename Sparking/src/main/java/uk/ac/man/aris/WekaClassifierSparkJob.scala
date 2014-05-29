package uk.ac.man.aris

import weka.classifiers.Classifier
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.ArrayList
import weka.core.Instances
import weka.core.Utils._
import weka.classifiers.Classifier._
import org.apache.spark.rdd.RDD

/**This job trains any classifier from the weka.classifiers._ package
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierSparkJob extends java.io.Serializable {
         
  /**Build and return the provided classifier
   * 
   * @param classifierToTrain is a string representing the classifier and the containing package ex: weka.classifiers.trees.J48
   * @param headers is the header file of the dataset
   * @param data is the RDD representation of the dataset 
   */
  def buildClassifier (metaL:String,classifierToTrain:String,classAtt:Int,headers:Instances,data:RDD[String]) : Classifier = {
         
         //compute the classifier: map produces classifiers on each partition and reduce aggregates the partition classifiers to a single
         val classifier=data.glom.map(new WekaClassifierSparkMapper(classAtt,metaL,classifierToTrain,null,null,headers).map(_)).reduce(new WekaClassifierSparkReducer(null).reduce(_,_))
         return classifier
  }
          
}