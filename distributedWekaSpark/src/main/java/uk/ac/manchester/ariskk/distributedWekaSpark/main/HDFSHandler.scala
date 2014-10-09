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
 *    HDFSHandler.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

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
class HDFSHandler (@transient val sc:SparkContext) extends java.io.Serializable{
  
  
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
  def loadRDDFromHDFS(path:String, partitions:Int):RDD[String]={
   if (partitions==0) return sc.textFile(path)
   return sc.textFile(path, partitions)
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
    rdd.saveAsTextFile(path)
    return true
  }
  
  /**Gets the size of a file in a path*/
  def getFileSize(path:String):Long={
    val URI=new URI(path)
    val fs=FileSystem.get(URI,sc.hadoopConfiguration)
    val filenamePath = new Path(path)
    return fs.getContentSummary(filenamePath).getLength;
  }
}