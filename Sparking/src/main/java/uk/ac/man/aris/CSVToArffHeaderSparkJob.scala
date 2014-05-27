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

/**  This Job builds Weka Headers for the provided dataset
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *                                           */

class CSVToArffHeaderSparkJob {
  //To-do: caching option + number of objects
  

  
   
  def buildHeaders (master:String,hdfsPath:String,numOfAttributes:Int,numberOfPartitions:Int) : Instances = {
     ///Config
      val conf=new SparkConf().setAppName("CSVToArffHeaderSparkJob").setMaster(master).set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      
      //dataloading
      val data=sc.textFile(hdfsPath,numberOfPartitions)
     
      
      //caching Only if this fits in memory! else either outofmem exception or need to implement caching stategy
          //data.persist(StorageLevel.MEMORY_AND_DISK)
          data.cache()
      // data.persist(StorageLevel.DISK_ONLY)
     
      //headers
     var names=new ArrayList[String]
     for (i <- 1 to numOfAttributes){
       names.add("att"+i)
     }
      
      //compute headers
       val headers=data.glom.map(new CSVToArffHeaderSparkMapper(null).map(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
       println(headers.toString)
       return headers
  }

    
}