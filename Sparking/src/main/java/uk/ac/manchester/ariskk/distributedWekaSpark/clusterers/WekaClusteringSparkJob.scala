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
  def buildClusterer(dataset:RDD[String],header:Instances,clustererToTrain:String,options:Array[String],num:Int):Clusterer={
    
       val clusterer=dataset.glom.map(new WekaClusteringSparkMapper(header).map(_))
                                 .reduce(new WekaClusteringSparkReducer().reduce(_,_,num))
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
  def buildClusterer(dataset:RDD[Array[Instance]],header:Instances,clustererToTrain:String,options:Array[String],num:Int)
                                                                                      (implicit d: DummyImplicit):Clusterer={
    
//       val clusterer=dataset.glom.map(new WekaClusteringSparkMapper(header).map(_))
//                                 .reduce(new WekaClusteringSparkReducer().reduce(_,_))
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
  def buildClusterer(dataset:RDD[Instances],header:Instances,clustererToTrain:String,options:Array[String],num:Int)
                                                              (implicit d1:DummyImplicit, d2:DummyImplicit):Clusterer={
    
//       val clusterer=dataset.glom.map(new WekaClusteringSparkMapper(header).map(_))
//                                 .reduce(new WekaClusteringSparkReducer().reduce(_,_))
   return null
  }

}