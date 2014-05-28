package uk.ac.man.aris

import weka.distributed.WekaClassifierReduceTask
import weka.classifiers.Classifier
import java.util.ArrayList

class WekaClassifierSparkReducer (options:Array[String]) extends java.io.Serializable {

     var r_task=new WekaClassifierReduceTask
     //to-do minimum training fraction
  
  def reduce(classifierA:Classifier,classifierB:Classifier):Classifier={
       var list=new ArrayList[Classifier]
       list.add(classifierA)
       list.add(classifierB)
      // println(classifierA.toString())
       //a list of classifiers,to-do tranining instances each classifier has, force vote(for aggregatables)
       return r_task.aggregate(list)
     }
}