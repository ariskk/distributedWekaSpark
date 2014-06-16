package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierReduceTask
import weka.classifiers.Classifier
import java.util.ArrayList


/**Reducer class for the classifier training job
 * 
 * Commutative and associative function that takes two classifiers, aggregates them (merging them if aggregatable or forming a voted ensemlbe if not)
 * and produces one classifier
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com) 
 */
class WekaClassifierSparkReducer (options:Array[String]) extends java.io.Serializable {
     
     //Base distributedWeka task. Contains the Classifier aggregator
     var r_task=new WekaClassifierReduceTask
     
     
  /** Reducer: aggregated classifiers 
   *  
   *  Classifier A and B can exchange roles with no difference in the outcome
   *  @param classifierA Aggregated classifiers so far
   *  @param classifierB The next classifier to aggregate
   *  @return the aggregated classifier*/
  def reduce(classifierA:Classifier,classifierB:Classifier):Classifier={
       var list=new ArrayList[Classifier]
       list.add(classifierA)
       list.add(classifierB)
       return r_task.aggregate(list)
     }
 }