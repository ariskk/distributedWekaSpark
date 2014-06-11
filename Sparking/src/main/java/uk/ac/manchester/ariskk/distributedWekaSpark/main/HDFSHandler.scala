package uk.ac.manchester.ariskk.distributedWekaSpark.main

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.fs.FileSystem._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataInput
import org.apache.hadoop.io.DataInputBuffer
import java.net.URI
import java.io.IOException



class HDFSHandler (sc:SparkContext) {
  
//maybe singleton
  
  /**ToDo: Save and load files to HDFS   */
  
  //working
  def loadFromHDFS(path:String,splits:Int):RDD[String]={
   return sc.textFile(path, splits)
 }
  
  //working
  def saveToHDFS(objectToSave:Object,path:String,key:String):Boolean={
    try{
    val URI=new URI(path)
    val fs=FileSystem.get(URI,sc.hadoopConfiguration)
    val testpath=new Path(path+"file123.csv")
    val stream=fs.create(testpath)
    stream.write(objectToSave.toString.getBytes())
    stream.flush()}
    catch {
      case ioe:IOException => println("failed! due to IOException")
      case e:Exception => println("random exception")
      case _ : Throwable=>  println("a random throwable")
      return false
    }
    

    return true
  }
  
  /**Method to save an RDD to HDFS*/
  def saveRDDToHDFS(rdd:RDD[String],path:String):Boolean={
    //To-Do: try-catch
    rdd.saveAsTextFile(path)
    return true
  }
}