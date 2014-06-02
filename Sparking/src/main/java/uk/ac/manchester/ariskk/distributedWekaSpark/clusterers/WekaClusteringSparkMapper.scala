package uk.ac.manchester.ariskk.distributedWekaSpark.clusterers

import weka.clusterers.Clusterer
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Instances
import weka.clusterers.SimpleKMeans
import weka.filters.unsupervised.instance.ReservoirSample

class WekaClusteringSparkMapper (header:Instances) extends java.io.Serializable{
  
   var m_rowparser=new CSVToARFFHeaderMapTask()
   
    //Remove the summary from the headers.
    val strippedHeader:Instances=CSVToARFFHeaderReduceTask.stripSummaryAtts(header)
    m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
  
    
    //Convert text to clusterer as in classifiers
    val clusterer=new SimpleKMeans
    
    
  def map(rows:Array[String]):Clusterer={
    val reservoir=new ReservoirSample
    
    for(x<-rows){
      val inst=m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x))
      header.add(inst)
      //add a reservoir. if updatable update else in the finalise. finaleze calls buildClusterer
     }
    clusterer.buildClusterer(header)
    //finalize tasks (build for batch no-op for incremental
    
    return null
  }

}