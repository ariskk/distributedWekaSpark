package uk.ac.manchester.ariskk.distributedWekaSpark.main

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask

class WekaInstancesRDDBuilder extends java.io.Serializable{
  
   var m_rowparser=new CSVToARFFHeaderMapTask()
//   val strippedHeader:Instances=CSVToARFFHeaderReduceTask.stripSummaryAtts(header)
//    strippedHeader.setClassIndex(classIndex)
//    m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
   
  def mappy(rows:Array[String],head:Instances):Instances={
     val stripped= CSVToARFFHeaderReduceTask.stripSummaryAtts(head) 
     stripped.setClassIndex(11)
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(stripped))
     var instances=new Instances(stripped)
       
       for (x <- rows){
         instances.add(m_rowparser.makeInstance(stripped, true, m_rowparser.parseRowOnly(x)))
         
       }
       return instances
     }

}