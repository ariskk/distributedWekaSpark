package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.clusterers.Canopy

class WekaClusteringSparkReducer extends java.io.Serializable{
  
  
  def reduce(clustA:Canopy,clustB:Canopy):Canopy={
    //clastA.aggregate(clustB....)
    
    return null
  }

}