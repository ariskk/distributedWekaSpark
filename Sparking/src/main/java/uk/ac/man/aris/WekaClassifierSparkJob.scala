package uk.ac.man.aris

import weka.classifiers.Classifier
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.ArrayList
import weka.core.Instances
import org.apache.spark.rdd.RDD

class WekaClassifierSparkJob extends java.io.Serializable {
  
  
  def buildClassifier (master:String,hdfsPath:String,numOfAttributes:Int,numberOfPartitions:Int,headers:Instances,data:RDD[String]) : Classifier = {
     ///Config
      var arffHeaders:Instances=headers
//      val conf=new SparkConf().setAppName("CSVToArffHeaderSparkJob").setMaster(master).set("spark.executor.memory","1g")
//      val sc=new SparkContext(conf)
//      
//      //dataloading
//      val data=sc.textFile(hdfsPath,numberOfPartitions)
//      data.cache()
      
       
//        if(arffHeaders==null){
//         val headerJob=new CSVToArffHeaderSparkJob
//        arffHeaders=headerJob.buildHeaders(master,hdfsPath,numOfAttributes,numberOfPartitions,null)
//        }
         
         val classifier=data.glom.map(new WekaClassifierSparkMapper(null,null,null,arffHeaders).map(_)).reduce(new WekaClassifierSparkReducer(null).reduce(_,_))
         return classifier
  }
    
      
}