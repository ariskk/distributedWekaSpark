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
import java.io.ObjectOutputStream
import java.io.FileOutputStream
import weka.core.SerializationHelper
import java.io.ObjectInputStream


/**A class to handle HDFS i/o
 * 
 * Contains methods to read/write RDDs as well as serialize/deserialize any other type of class 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com) 
 */
class HDFSHandler (sc:SparkContext) {
  
//maybe singleton
  
  /**Loads a serialized object from HDFS  */
  def loadObjectFromHDFS(path:String):Object={
    val URI=new URI(path)
    val fs=FileSystem.get(URI,sc.hadoopConfiguration)
    val inStream=fs.open(new Path(path+"file123.csv"))
    val br=new BufferedReader(new InputStreamReader(inStream))
    val ois = new ObjectInputStream(inStream)
    return ois.readObject()
  }
  
   /**Loads a file from HDFS as an RDD*/
  def loadRDDFromHDFS(path:String, splits:Int):RDD[String]={
   return sc.textFile(path, splits)
 }
  
   /**Serializes and Saves an object to HDFS*/
  def saveObjectToHDFS(objectToSave:Object, path:String, key:String):Boolean={
    try{
      val URI=new URI(path)
      val fs=FileSystem.get(URI,sc.hadoopConfiguration)
      val testpath=new Path(path+"file123.csv")
      val stream=fs.create(testpath)
      val serializer=SerializationHelper.write(stream, objectToSave)

      }
    catch {
      case _ : Throwable=>  println("Exception caught while trying to save"+objectToSave.toString()+" to HDFS!")
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