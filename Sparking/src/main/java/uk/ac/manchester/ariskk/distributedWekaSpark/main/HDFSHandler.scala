package uk.ac.manchester.ariskk.distributedWekaSpark.main

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.fs.FileSystem._
import org.apache.hadoop.fs.FileSystem
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils


class HDFSHandler (sc:SparkContext) {
  
//maybe singleton
  
  /**ToDo: Save and load files to HDFS   */
  
  def loadFromHDFS(path:String,splits:Int):RDD[String]={
   return sc.textFile(path, splits)
 }
  
  def saveToHDFS(objectToSave:Object,path:String,key:String):Boolean={
    val fs=FileSystem.get(sc.hadoopConfiguration)
    val toWrite=objectToSave.toString
    val in=new BufferedInputStream(new ByteArrayInputStream(toWrite.getBytes()))
    val tohdfs=new Path(path)
    val out=fs.append(tohdfs)
   
    IOUtils.copyBytes(in,out,sc.hadoopConfiguration)
    return true
  }
  
  def saveRDDToHDFS(rdd:RDD[String],path:String):Boolean={
    rdd.saveAsTextFile(path)
    return true
  }
}