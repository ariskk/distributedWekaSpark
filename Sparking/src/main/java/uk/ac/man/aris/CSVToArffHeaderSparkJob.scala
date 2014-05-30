package uk.ac.man.aris

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import weka.classifiers.bayes.net.search.fixed.NaiveBayes
import weka.distributed.WekaClassifierMapTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import java.util.ArrayList
import weka.core.Instances
import org.apache.spark.rdd.RDD

/**  This Job builds Weka Headers for the provided dataset
 *   
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class CSVToArffHeaderSparkJob {
  //To-do: caching option + number of objects
  
 /**Build the Header file
    * 
    * @param numOfAttributes the number of attributes in the dataset
    * @param data is a reference to the RDD of the dataset
    * @return the headers (weka.core.Instances object)
    *   */
  def buildHeaders (names:ArrayList[String],numOfAttributes:Int,data:RDD[String]) : Instances = {
     
    //generate headers' names if not provided
     if(names.size()==0){
     for (i <- 1 to numOfAttributes){
       names.add("att"+i)
     }}
     //compute headers using map(generate headers for each partition) and reduce (aggregate partial headers)
     val headers=data.glom.map(new CSVToArffHeaderSparkMapper(null).map(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
     return headers
  }
    }