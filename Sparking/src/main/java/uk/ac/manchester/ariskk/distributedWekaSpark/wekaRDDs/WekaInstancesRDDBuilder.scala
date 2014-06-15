package uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask

class WekaInstancesRDDBuilder extends java.io.Serializable {
  
  var m_rowparser=new CSVToARFFHeaderMapTask()

   
  def map(rows:Array[String], head:Instances):Instances={
     val stripped= CSVToARFFHeaderReduceTask.stripSummaryAtts(head) 
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(stripped))
     var instances=new Instances(stripped)
       
       for (x <- rows){
         instances.add(m_rowparser.makeInstance(stripped, true, m_rowparser.parseRowOnly(x)))   
       }
       return instances
     }

}