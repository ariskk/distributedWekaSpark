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



object SimpleSparkApp {
   def main(args : Array[String]){
    
     val headerjob=new CSVToArffHeaderSparkJob
     val header=headerjob.buildHeaders("local[4]", "hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv", 12, 4,null)
     val classifierjob=new WekaClassifierSparkJob
     val classifier=classifierjob.buildClassifier("local[4]", "hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv", 12, 4, header,headerjob.getd) 
      println(classifier.toString())
   }
   
 
}