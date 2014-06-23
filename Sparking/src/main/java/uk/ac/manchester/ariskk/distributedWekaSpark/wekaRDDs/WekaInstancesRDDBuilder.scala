package uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Utils

/**Class that contains a map tasks which produces an Instances object from an Array[String]
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaInstancesRDDBuilder (headers:Instances) extends java.io.Serializable {
  
  var m_rowparser=new CSVToARFFHeaderMapTask()
  val stripped= CSVToARFFHeaderReduceTask.stripSummaryAtts(headers) 
  m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(stripped))
  
  //val split=Utils.splitOptions("-N first-last")
 // m_rowparser.setOptions(split)
   
  def map(rows:Array[String]):Instances={
     
     
     var instances=new Instances(stripped)
       
       for (x <- rows){
         instances.add(m_rowparser.makeInstance(stripped, true, m_rowparser.parseRowOnly(x)))   
       }
       return instances
     }

}