package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierReduceTask
import weka.classifiers.Classifier
import java.util.ArrayList


/**Reducer class for the classifier training job
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com) 
 */
class WekaClassifierSparkReducer (options:Array[String]) extends java.io.Serializable {
     
     //Initialize the Base reduce task (Aggregates aggregatable classifiers or produces a voted ensemble for non-agg)
     var r_task=new WekaClassifierReduceTask
     
     
  /** Reducer: aggregated classifiers 
   *  
   *  @param classifierA Aggregated classifier so far
   *  @param classifierB The classifier to aggregate
   *  @return the aggregated classifier*/
  def reduce(classifierA:Classifier,classifierB:Classifier):Classifier={
       var list=new ArrayList[Classifier]
       list.add(classifierA)
       list.add(classifierB)
       return r_task.aggregate(list)
     }
 }