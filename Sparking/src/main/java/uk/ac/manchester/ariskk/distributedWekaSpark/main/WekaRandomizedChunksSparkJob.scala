package uk.ac.manchester.ariskk.distributedWekaSpark.main

import org.apache.spark.rdd.RDD
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import scala.util.Random
import weka.distributed.CSVToARFFHeaderReduceTask


class WekaRandomizedChunksSparkJob {
     var m_rowParser=new CSVToARFFHeaderMapTask

  //map reduce implmentation of randomized. Not sure I will use if though
  def randomize(dataset:RDD[String],numOfChunks:Int,headers:Instances,classIndex:Int): RDD[String]={
    val strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
    strippedHeaders.setClassIndex(classIndex)
    
    if(!strippedHeaders.classAttribute().isNominal()) {
    m_rowParser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
    var randomGen=new Random(1L)
    for(i<-1 to 10){randomGen.nextInt}  //throw away the first 10 numbers. They tend to be less random
    //ToDo: find a way to ensure each chunk has similar class distribution
    
    return dataset.repartition(numOfChunks)
    }
    else{
    return dataset.repartition(numOfChunks)
    }
  }

  def map(rows:Array[String],strippedHeaders:Instances): Array[String]={
    for(x<-rows){
      m_rowParser.makeInstance(strippedHeaders, true, m_rowParser.parseRowOnly(x))
      
    }
    return null
  }
  def reduce(rows:Array[String]) :RDD[String]={
    return null
  }
}