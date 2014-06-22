package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.clusterers.Canopy
import java.util.ArrayList
import weka.core.NormalizableDistance
import weka.core.EuclideanDistance
import weka.core.ChebyshevDistance
import weka.core.ManhattanDistance
import weka.core.MinkowskiDistance
import weka.core.Instances

class WekaClusteringSparkReducer(head:Instances) extends java.io.Serializable{
  
  
  def reduce(clustA:Canopy,clustB:Canopy,numofcanopies:Int):Canopy={
  
    val list=new ArrayList[Canopy]
    list.add(clustA)
    list.add(clustB)
    val dist=new EuclideanDistance(clustA.getCanopies())
    
    val aggregated=Canopy.aggregateCanopies(list, clustA.getActualT1(), clustA.getActualT2(), dist, null, 3)
    println(aggregated)
    return aggregated
  }

}