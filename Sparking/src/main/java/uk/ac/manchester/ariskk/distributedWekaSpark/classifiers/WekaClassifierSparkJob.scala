package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.classifiers.Classifier
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
   * @param parserOptions are options for the csvparser
   * @param classifierOptions are options for the classifier
   * @return a trained classifier 
   */
  def buildClassifier (metaLearner:String,classifierToTrain:String,classIndex:Int,headers:Instances,dataset:RDD[String],
                                                         parserOptions:Array[String],classifierOptions:Array[String]) : Classifier = {
         
       //compute the classifier: map produces a classifier for each partition and reduce aggregates the partition classifiers to a single output
      val classifier=dataset.glom.map(new WekaClassifierSparkMapper(classIndex,metaLearner,classifierToTrain,classifierOptions,parserOptions,headers).map(_))
                                  .reduce(new WekaClassifierSparkReducer(null).reduce(_,_))
                                  
      return classifier
  }
          
}