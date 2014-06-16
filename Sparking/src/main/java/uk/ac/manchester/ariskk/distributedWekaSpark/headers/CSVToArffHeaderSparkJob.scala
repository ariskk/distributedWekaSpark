package uk.ac.manchester.ariskk.distributedWekaSpark.headers
import java.util.ArrayList
import weka.core.Instances
import org.apache.spark.rdd.RDD
import weka.distributed.CSVToARFFHeaderMapTask


/**  This Job builds Weka Arff Headers using a provided dataset in RDD[String] 
 *   
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class CSVToArffHeaderSparkJob {
 
  
 /**Build the Header file
    * 
    * @param numOfAttributes the number of attributes in the dataset
    * @param data is a reference to the RDD of the dataset
    * @return the headers (weka.core.Instances object)
    *   */
  def buildHeaders (options:Array[String],names:ArrayList[String],numOfAttributes:Int,data:RDD[String]) : Instances = {
     var headers:Instances=null
     //generate headers' names if not provided
     if(names.size()==0){
     for (i <- 1 to numOfAttributes){
       names.add("att"+i)
     }}
     
     
     val tempMapTask=new CSVToARFFHeaderMapTask()
     tempMapTask.setOptions(options)
     
     //Check if headers are available immediately (in case the user specified that there are no nominal attributes (-no-summary-stats option was provided)
     if(tempMapTask.headerAvailableImmediately(names.size, names, new StringBuffer) &&(! tempMapTask.getComputeSummaryStats())){
       headers=tempMapTask.getHeader(names.size, names)
     }
     else{
     //compute headers using map(generate headers for each partition) and reduce (aggregate partial headers)
     headers=data.glom.map(new CSVToArffHeaderSparkMapper(options).map(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
     }
     return headers
   }
 }