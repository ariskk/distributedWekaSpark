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
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *                                           */

class CSVToArffHeaderSparkJob {
  //To-do: caching option + number of objects
  
     var data2:RDD[String]=null
  
   
  def buildHeaders (numOfAttributes:Int,data:RDD[String]) : Instances = {
     
       
     
      //generate headers' names if not provided
     var names=new ArrayList[String]
     for (i <- 1 to numOfAttributes){
       names.add("att"+i)
     }
      
      //compute headers
       val headers=data.glom.map(new CSVToArffHeaderSparkMapper(null).map(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
       println(headers.toString)
       
       //cleanup ? or pass a reference to the next task
        
       return headers
  }

     def getd():RDD[String]= data2
    
}