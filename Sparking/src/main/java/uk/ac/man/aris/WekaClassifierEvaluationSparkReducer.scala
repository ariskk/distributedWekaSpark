package uk.ac.man.aris

import weka.distributed.WekaClassifierEvaluationReduceTask
import weka.classifiers.evaluation.Evaluation
import java.util.ArrayList

class WekaClassifierEvaluationSparkReducer {
  var r_task=new WekaClassifierEvaluationReduceTask
  
  def reduce(evalA:Evaluation,evalB:Evaluation): Evaluation={
    val list=new ArrayList[Evaluation]
    list.add(evalA)
    list.add(evalB)
    return r_task.aggregate(list)
  }
  
}