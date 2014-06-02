package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.core.Instances
import org.apache.spark.rdd.RDD

class WekaClustererSparkJob extends java.io.Serializable{
  
  
  def buildClusterer(header:Instances,clustererToTrain:String,options:Array[String],dataset:RDD[String]):Clusterer={
    
       val clusterer=dataset.glom.map(new WekaClusteringSparkMapper(header).map(_))
                                 .reduce(new WekaClusteringSparkReducer().reduce(_,_))
   return clusterer
  }

}