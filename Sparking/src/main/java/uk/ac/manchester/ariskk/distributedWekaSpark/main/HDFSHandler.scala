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
import java.io.BufferedReader
import java.io.InputStreamReader
import weka.core.Instances
import weka.classifiers.Classifier



class HDFSHandler (sc:SparkContext) {
  
//maybe singleton
  
  /**ToDo: Save and load files to HDFS   */
  def loadObjectFromHDFS(path:String):Object={
    val URI=new URI(path)
    val fs=FileSystem.get(URI,sc.hadoopConfiguration)
    val inStream=fs.open(new Path(path+"file123.csv"))
    val br=new BufferedReader(new InputStreamReader(inStream))
    println("so far so good")
    val inst=new Instances(br)
    //val cl=new Classifier()
    println(inst.numAttributes())
    println(inst)
    var line=""
    var next=""
      while(next!=null){
        
        line+=next+"\n"
     //   println(br.readLine)
        next=br.readLine
      }
       println(line)
       println("dadsdsff")
   // val obj=inStream.
    return line
  }
  //working
  def loadRDDFromHDFS(path:String, splits:Int):RDD[String]={
   return sc.textFile(path, splits)
 }
  
  //working
  def saveObjectToHDFS(objectToSave:Object, path:String, key:String):Boolean={
    try{
      val URI=new URI(path)
      val fs=FileSystem.get(URI,sc.hadoopConfiguration)
      val testpath=new Path(path+"file123.csv")
      val stream=fs.create(testpath)
      stream.write(objectToSave.toString.getBytes())
      stream.close()
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