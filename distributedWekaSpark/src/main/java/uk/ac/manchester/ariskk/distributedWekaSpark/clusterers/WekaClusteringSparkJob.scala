/*
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 *    WekaClusteringSparkJob.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.core.Instances
import org.apache.spark.rdd.RDD
import weka.core.Instance

/**Spark Job for building Weka's Clusterers
 * 
 * This Job can only be used on Canopy Clusterers at the moment
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
  def buildClusterer(dataset:RDD[String],header:Instances,clustererToTrain:String,options:Array[String],numClusters:Int,distanceMetric:String):Clusterer={
    
       val clusterer=dataset.glom.map(new WekaClusteringSparkMapper(header,options).map(_))
                                 .reduce(new WekaClusteringSparkReducer(header,distanceMetric).reduce(_,_,numClusters))
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
  def buildClusterer(dataset:RDD[Array[Instance]],header:Instances,clustererToTrain:String,options:Array[String],numClusters:Int,distanceMetric:String)
                                                                                      (implicit d: DummyImplicit):Clusterer={
    
       val clusterer=dataset.map(new WekaClusteringSparkMapper(header,options).map(_))
                                 .reduce(new WekaClusteringSparkReducer(header,distanceMetric).reduce(_,_,numClusters))
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
  def buildClusterer(dataset:RDD[Instances],header:Instances,clustererToTrain:String,options:Array[String],numClusters:Int,distanceMetric:String)
                                                              (implicit d1:DummyImplicit, d2:DummyImplicit):Clusterer={
    
      val clusterer=dataset.map(new WekaClusteringSparkMapper(header,options).map(_))
                                .reduce(new WekaClusteringSparkReducer(header,distanceMetric).reduce(_,_,numClusters))
   return null
  }

}