package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.clusterers.Canopy
import java.util.ArrayList

class WekaClusteringSparkReducer extends java.io.Serializable{
  
  
  def reduce(clustA:Canopy,clustB:Canopy,numofcanopies:Int):Canopy={
    val list=new ArrayList[Canopy]
    list.add(clustA)
    list.add(clustB)
    
    val aggregated=Canopy.aggregateCanopies(list, clustA.getActualT1(), clustB.getActualT2(), null, null, numofcanopies)

    return aggregated
  }

}