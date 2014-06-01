package uk.ac.manchester.ariskk.distributedWekaSpark.headers

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask
import java.util.ArrayList

/**Reducer implementation for CSVToArffHeaderSpark job 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   */


class CSVToArffHeaderSparkReducer extends java.io.Serializable {

  var r_task=new CSVToARFFHeaderReduceTask
  
  
  /**   Spark  wrapper for CSVToArffMapTask base task
 *    @param HeaderA represents aggregated headers
 *    @param HeaderB the next Header to aggregate
 *    @return Aggregated headers         */
  def reduce (headerA:Instances,headerB:Instances): Instances ={
    var list=new ArrayList[Instances]
    list.add(headerA)
    list.add(headerB)     
    return r_task.aggregate(list)
  }
}