package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierEvaluationReduceTask
import weka.classifiers.evaluation.Evaluation
import java.util.ArrayList
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask._
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.distributed.CSVToARFFHeaderMapTask


/**Classifier evaluation Reduce task
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)*/
class WekaClassifierEvaluationSparkReducer  extends java.io.Serializable {
  
  //Initialize Base reduce task
  var r_task=new WekaClassifierEvaluationReduceTask
  

  /**Reducer that merges two evaluations
   * 
   * @param evalA evaluations merged so far
   * @param evalB next evaluation to merge
   * @return aggregated evaluations
   * */
  def reduce(evalA:Evaluation,evalB:Evaluation): Evaluation={
    val list=new ArrayList[Evaluation]
    list.add(evalA)
    list.add(evalB)
    return r_task.aggregate(list)
  }
  
}