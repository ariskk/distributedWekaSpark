package uk.ac.man.aris

import weka.classifiers.Classifier
import org.apache.spark.rdd.RDD
import weka.core.Instances
import weka.classifiers.evaluation.Evaluation
import java.util.ArrayList
import weka.core.Attribute

/**Spark Job for running an evaluation job on a trained classifier or regressor
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierEvaluationSparkJob extends java.io.Serializable{
  
  
  /** Evaluate the provided clasisifer or regressor
   *  
   *  @param classifier is the trained classifier
   *  @param headers is the headers object
   *  @param dataset is an RDD representation of the dataset
   *  @return an Evaluation object
   */
  def evaluateClassifier (classifier:Classifier,headers:Instances,dataset:RDD[String]): Evaluation={
    
    val eval=dataset.glom.map(new WekaClassifierEvaluationSparkMapper(headers,classifier).map(_)).reduce(new WekaClassifierEvaluationSparkReducer(headers).reduce(_,_))
    return eval
  }

  def evaluateFoldBasedClassifier(folds:Int,classifier:Classifier,headers:Instances,dataset:RDD[String]):Evaluation={
  //  val eval=dataset.glom.map(new WekaClassifierFoldBasedEvaluationSparkMapper(headers,classifier,folds).map(_)).reduce(new WekaClassifierEvaluationSparkReducer(headers).reduce(_, _))
    return null
  }
  
  
  
   /** A method to display the evaluation results
    *  
    *   @param an Evaluation object */
   def displayEval(aggregated:Evaluation):Unit={
    val results=new ArrayList[Double]
    results.add(aggregated.correct())
    results.add(aggregated.incorrect())
    results.add(aggregated.meanAbsoluteError())
    results.add(aggregated.rootMeanSquaredError())
    results.add(aggregated.relativeAbsoluteError())
    results.add(aggregated.rootRelativeSquaredError())
    results.add(aggregated.numInstances())
    
    val atts = new ArrayList[Attribute];
    atts.add(new Attribute("Correctly classified instances"));
    atts.add(new Attribute("Incorrectly classified instances"));
    atts.add(new Attribute("Mean absolute error"));
    atts.add(new Attribute("Root mean squared error"));
    atts.add(new Attribute("Relative absolute error"));
    atts.add(new Attribute("Root relative squared error"));
    atts.add(new Attribute("Total number of instances"));
     
    for(x <- 0 to atts.size-1){
      println(atts.get(x).toString()+ "  "+results.get(x))
      
    }
     
     
     
   }
}