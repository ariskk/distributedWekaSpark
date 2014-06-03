package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.core.Instances
import org.apache.spark.rdd.RDD

/**Spark Job for building Weka's Clusterers
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClustererSparkJob extends java.io.Serializable{
  
  /**Method that builds a clustering on a dataset
   * 
   * @param header is the dataset header
   * @param clustererToTrain the clustering algorithm to use
   * @param options clusterer options
   * @param dataset to process
   * @return a weka clusterer
   */
  def buildClusterer(header:Instances,clustererToTrain:String,options:Array[String],dataset:RDD[String]):Clusterer={
    
       val clusterer=dataset.glom.map(new WekaClusteringSparkMapper(header).map(_))
                                 .reduce(new WekaClusteringSparkReducer().reduce(_,_))
   return clusterer
  }

}