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
 *    WekaClusteringSparkMapper.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Instances
import weka.clusterers.SimpleKMeans
import weka.filters.unsupervised.instance.ReservoirSample
import weka.clusterers.Canopy
import weka.core.Instance

class WekaClusteringSparkMapper (header:Instances,options:Array[String]) extends java.io.Serializable{
  
   var m_rowparser=new CSVToARFFHeaderMapTask()
   
    //Remove the summary from the headers.
    val strippedHeader:Instances=CSVToARFFHeaderReduceTask.stripSummaryAtts(header)
    m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
  
    
    //Convert text to clusterer as in classifiers: Only Canopy currently supported
    val clusterer=new Canopy
    clusterer.setOptions(options)

    
  def map(rows:Array[String]):Canopy={
    
    
    for(x<-rows){
      val inst=m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x))
      
      header.add(inst)
      
     }
     clusterer.buildClusterer(header)

    
    return clusterer
  }
   
   def map(rows:Array[Instance]):Canopy={
    
     for(x<-rows){
      header.add(x)
     
     }
     clusterer.buildClusterer(header)
     
    
    return clusterer
  }
   
   def map(instances:Instances):Canopy={
    
     clusterer.buildClusterer(instances)
    
    return clusterer
  }

}