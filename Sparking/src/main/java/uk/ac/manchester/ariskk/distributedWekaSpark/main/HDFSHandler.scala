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
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import org.glassfish.grizzly.streams.BufferedOutput
import java.io.DataInput
import org.apache.hadoop.io.DataInputBuffer


class HDFSHandler (sc:SparkContext) {
  
//maybe singleton
  
  /**ToDo: Save and load files to HDFS   */
  
  //working
  def loadFromHDFS(path:String,splits:Int):RDD[String]={
   return sc.textFile(path, splits)
 }
  
  //not: permissions issues?
  def saveToHDFS(objectToSave:Object,path:String,key:String):Boolean={
    val fs=FileSystem.get(sc.hadoopConfiguration)
    val toWrite=objectToSave.toString
  //  val in=new BufferedInputStream(new ByteArrayInputStream(toWrite.getBytes()))
    val in=new DataInputBuffer
    in.read(toWrite.getBytes())
    val tohdfs=new Path(path)
    //val out=fs.append(tohdfs)
    val out2=new DataOutputBuffer
    out2.write(in,in.getLength())
    sc.hadoopConfiguration.write(out2)
    IOUtils.copyBytes(in,out2,sc.hadoopConfiguration)
    return true
  }
  
  //working
  def saveRDDToHDFS(rdd:RDD[String],path:String):Boolean={
    rdd.saveAsTextFile(path)
    return true
  }
}