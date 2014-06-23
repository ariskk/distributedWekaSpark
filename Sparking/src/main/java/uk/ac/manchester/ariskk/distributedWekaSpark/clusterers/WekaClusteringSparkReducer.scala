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

class WekaClusteringSparkReducer(head:Instances,distance:String) extends java.io.Serializable{
  
  var normalizedDistance:NormalizableDistance=null
  
  distance match{
    
    case "ManhattanDistance" => normalizedDistance=new ManhattanDistance
    case "MinKowskiDistance" => normalizedDistance=new MinkowskiDistance
    case "ChebyshenDistance" => normalizedDistance=new ChebyshevDistance
    case _                   => normalizedDistance=new EuclideanDistance
  }
  
  def reduce(clustA:Canopy,clustB:Canopy,numofcanopies:Int):Canopy={
    
    val list=new ArrayList[Canopy]
    list.add(clustA)
    list.add(clustB)
    //val dist=new distance(clustA.getCanopies())
    normalizedDistance.setInstances(clustA.getCanopies())
    val aggregated=Canopy.aggregateCanopies(list, clustA.getActualT1(), clustA.getActualT2(), normalizedDistance, null, numofcanopies)
    println(aggregated)
    return aggregated
  }

}