package uk.ac.man.aris

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask
import java.util.ArrayList

class CSVToArffHeaderSparkReducer extends java.io.Serializable {

  var r_task=new CSVToARFFHeaderReduceTask
  
  
  
  def reduce (headerA:Instances,headerB:Instances): Instances ={
    var list=new ArrayList[Instances]
    list.add(headerA)
    list.add(headerB)
    return r_task.aggregate(list)
  }
}