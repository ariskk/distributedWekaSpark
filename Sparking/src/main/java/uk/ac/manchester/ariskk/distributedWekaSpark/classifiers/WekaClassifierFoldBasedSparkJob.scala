package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.core.Instances
import org.apache.spark.rdd.RDD
import weka.classifiers.Classifier
import weka.distributed.WekaClassifierReduceTask



/**Spark Job for training an arbitrary number of folds on a classifier
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedSparkJob extends java.io.Serializable{
  
  
  
  /**Method to build a fold-based model of a classifier
   * 
   * @param dataset is the dataset to process
   * @param headers headers file for the dataset
   * @param folds is the number of folds
   * @param classifierToTrain is the classifier
   * @param metaLearner - optional metaLearner
   * @return a trained classifier*/
  def buildFoldBasedModel(dataset:RDD[String],headers:Instances,folds:Int,classifierToTrain:String,metaLearner:String,classIndex:Int): Classifier={
    

     val classifiers=dataset.glom.map(new WekaClassifierFoldBasedSparkMapper(folds,headers,classifierToTrain,metaLearner,classIndex).map(_))
                                .reduce(new WekaClassifierFoldBasedSparkReducer(folds).reduce(_,_))   
                                
   return new WekaClassifierReduceTask().aggregate(classifiers)
  }

}