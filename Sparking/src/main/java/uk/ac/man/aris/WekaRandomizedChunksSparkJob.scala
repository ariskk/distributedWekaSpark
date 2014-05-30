package uk.ac.man.aris

import org.apache.spark.rdd.RDD
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import scala.util.Random
import weka.distributed.CSVToARFFHeaderReduceTask


class WekaRandomizedChunksSparkJob {
     var m_rowParser=new CSVToARFFHeaderMapTask

  //map reduce implmentation of randomized. Not sure I will use if though
  def randomize(dataset:RDD[String],numOfChunks:Int,headers:Instances,classIndex:Int): RDD[String]={
    var strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
    strippedHeaders.setClassIndex(classIndex)
    
    if(!strippedHeaders.classAttribute().isNominal()) {
    m_rowParser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
    var randomGen=new Random(1L)
    for(i<-1 to 10){randomGen.nextInt}  //throw away the first 10 numbers. They tend to be less random
    
    
    return null
    }
    else{
    return dataset.repartition(numOfChunks)
    }
  }

  def map(rows:Array[String]): Array[String]={
    return null
  }
  def reduce(rows:Array[String]) :RDD[String]={
    return null
  }
}