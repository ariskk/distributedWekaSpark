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
 *    WekaClusteringSparkReducer.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

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
    case  _                   => normalizedDistance=new EuclideanDistance
  }
  
  def reduce(clustA:Canopy,clustB:Canopy,numofcanopies:Int):Canopy={
    
    val list=new ArrayList[Canopy]
    list.add(clustA)
    list.add(clustB)
    //val dist=new distance(clustA.getCanopies())
    normalizedDistance.setInstances(clustA.getCanopies())
    val aggregated=Canopy.aggregateCanopies(list, clustA.getActualT1(), clustA.getActualT2(), normalizedDistance, null, numofcanopies)
    //println(aggregated)
    return aggregated
  }

}