package uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Instance

/**Class that contains a map that parses an Array[String] and produces an Array[Instance]
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaInstanceArrayRDDBuilder extends java.io.Serializable {
  
  var m_rowparser=new CSVToARFFHeaderMapTask()

   
  def map(rows:Array[String], head:Instances):Array[Instance]={
     val stripped= CSVToARFFHeaderReduceTask.stripSummaryAtts(head) 
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(stripped))
     var instanceArray=new Array[Instance](rows.length)
     var j=0  
     for (x <- rows){
         instanceArray(j)=m_rowparser.makeInstance(stripped, true, m_rowparser.parseRowOnly(x))
         j+=1
      }
       return instanceArray
     }

}
  
  
