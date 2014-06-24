package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.core.Instances
import org.apache.spark.rdd.RDD
import weka.core.Instance

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
  def buildClusterer(dataset:RDD[String],header:Instances,clustererToTrain:String,options:Array[String],numClusters:Int):Clusterer={
    
       val clusterer=dataset.glom.map(new WekaClusteringSparkMapper(header,options).map(_))
                                 .reduce(new WekaClusteringSparkReducer(header,null).reduce(_,_,numClusters))
   return clusterer
  }
  
    /**Method that builds a clustering on a dataset. Accepts dataset in RDD[Array[Instance]]
   * 
   * @param header is the dataset header
   * @param clustererToTrain the clustering algorithm to use
   * @param options clusterer options
   * @param dataset to process
   * @return a weka clusterer
   */
  def buildClusterer(dataset:RDD[Array[Instance]],header:Instances,clustererToTrain:String,options:Array[String],numClusters:Int)
                                                                                      (implicit d: DummyImplicit):Clusterer={
    
       val clusterer=dataset.map(new WekaClusteringSparkMapper(header,options).map(_))
                                 .reduce(new WekaClusteringSparkReducer(header,null).reduce(_,_,numClusters))
   return null
  }
  
    /**Method that builds a clustering on a dataset. Accepts dataset in RDD[Intances]
   * 
   * @param header is the dataset header
   * @param clustererToTrain the clustering algorithm to use
   * @param options clusterer options
   * @param dataset to process
   * @return a weka clusterer
   */
  def buildClusterer(dataset:RDD[Instances],header:Instances,clustererToTrain:String,options:Array[String],numClusters:Int)
                                                              (implicit d1:DummyImplicit, d2:DummyImplicit):Clusterer={
    
      val clusterer=dataset.map(new WekaClusteringSparkMapper(header,options).map(_))
                                .reduce(new WekaClusteringSparkReducer(header,null).reduce(_,_,numClusters))
   return null
  }

}