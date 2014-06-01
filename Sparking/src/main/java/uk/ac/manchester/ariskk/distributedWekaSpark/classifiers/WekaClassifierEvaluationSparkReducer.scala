package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierEvaluationReduceTask
import weka.classifiers.evaluation.Evaluation
import java.util.ArrayList
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask._
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.distributed.CSVToARFFHeaderMapTask

class WekaClassifierEvaluationSparkReducer (headers:Instances) extends java.io.Serializable {
  
  var r_task=new WekaClassifierEvaluationReduceTask
  //might be optional-- update:IS optional
  val strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
  strippedHeaders.setClassIndex(11)
  val classAt=strippedHeaders.classAttribute()
  val classAtSummaryName=CSVToARFFHeaderMapTask.ARFF_SUMMARY_ATTRIBUTE_PREFIX+classAt.name()
  val classSummaryAt=headers.attribute(classAtSummaryName)
  
  def reduce(evalA:Evaluation,evalB:Evaluation): Evaluation={
    val list=new ArrayList[Evaluation]
    list.add(evalA)
    list.add(evalB)
    return r_task.aggregate(list)
  }
  
}