package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.core.Instances
import org.apache.spark.rdd.RDD
import weka.classifiers.Classifier
import weka.distributed.WekaClassifierReduceTask



/**Spark Job for training an arbitrary number of folds in a classifier/regressor
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedSparkJob extends java.io.Serializable{
  
  
  
  
  def buildFoldBasedModel(dataset:RDD[String],headers:Instances,folds:Int,toTrain:String,metaL:String): Classifier={
    

  val classifiers=dataset.glom.map(new WekaClassifierFoldBasedSparkMapper(folds,headers,toTrain,metaL).map(_)).reduce(new WekaClassifierFoldBasedSparkReducer(folds).reduce(_,_))   
   return new WekaClassifierReduceTask().aggregate(classifiers)}

}