package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer

class WekaClusteringSparkReducer extends java.io.Serializable{
  
  
  def reduce(clustA:Clusterer,clustB:Clusterer):Clusterer={
    //consensus clusterer aggregating clusterers somehow. this is tricky
    
    return null
  }

}