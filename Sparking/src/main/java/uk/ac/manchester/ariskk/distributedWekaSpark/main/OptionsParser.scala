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
 *    OptionsParser.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.main


import java.util.ArrayList
import weka.distributed.DistributedWekaException
import weka.core.Utils
import weka.distributed.DistributedWekaException
import org.apache.spark.storage.StorageLevel

/**Class that parses the String of user-provided options and has methods to return ind
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class OptionsParser (options:String) extends java.io.Serializable{
  
  //!!!!Utils.getOption(...) can be called ONLY ONCE. the next time it returns empry!!!
  
  
  //String containing user provided options of the format "-option-type1 optionValue1 -option-type2 optionValue2"
  val split=Utils.splitOptions(options)
  
  /**Get a string that describes the user requested task.eg: classification,clustering etc ++*/
  def getTask():String={
    val task=Utils.getOption("task",split)
    if(task=="") throw new DistributedWekaException("Empty Task Identifier!")
    return task
  }
  
  /**Get a string that describes the type of the dataset to process. Options: Instances, ArrayInstance, ArrayString*/
  def getDatasetType():String={
    val dstype=Utils.getOption("dataset-type",split)
    if (dstype=="") return "ArrayString"
    return dstype
  }
  
  /**Get the user requested caching strategy*/
  def getCachingStrategy():StorageLevel={
    val strategy=Utils.getOption("caching",split)
    strategy match {
      case "MEMORY_ONLY" => return StorageLevel.MEMORY_ONLY
      case "MEMORY_AND_DISK" => return StorageLevel.MEMORY_AND_DISK
      case "MEMORY_ONLY_SER" => return StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK_SER" => return StorageLevel.MEMORY_AND_DISK_SER
      case "DISK_ONLY" => return StorageLevel.DISK_ONLY
      case "MEMORY_ONLY_2" => return StorageLevel.MEMORY_ONLY_2
      case "MEMORY_AND_DISK_2" => return StorageLevel.MEMORY_AND_DISK_2
      case _ => println("Not recognised or supported caching strategy requested! Will use MEMORY_AND_DISK instead"); return StorageLevel.MEMORY_AND_DISK
    }
    
  }

  /**Spark Master adress*/
  def getMaster():String={
    val master=Utils.getOption("master",split)
    if (master==""){println("Master not provided. The tasks will be executed locally.");return "local[4]"}
    else return master
  }
  
  /**HDFS path to the dataset*/
  def getHdfsDatasetInputPath():String={
    val hdfsPath=Utils.getOption("hdfs-dataset-path",split)
    if (hdfsPath=="")  return "hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv"
    return hdfsPath
  }
  
    /**Returns an hdfs path to the names file*/
  def getNamesPath():String={
    val namespath=Utils.getOption("names-path",split)
    if (namespath=="") return "hdfs://sandbox.hortonworks.com:8020/user/weka/namessupermarket"
    else return namespath
  }
  
  def getHdfsHeadersInputPath():String={
   val headersPath=Utils.getOption("hdfs-headers-path",split)
   return headersPath
    
  }
  def getHdfsClassifierInputPath():String={
     val classifierPath=Utils.getOption("hdfs-classifier-path",split) 
     return classifierPath
  }
  
  def getHdfsClustererInputPath():String={
    return null
    
  }
  
  
  def getHdfsOutputPath():String={
    val outpath=Utils.getOption("hdfs-output-path",split)
    if (outpath=="") return "hdfs://sandbox.hortonworks.com:8020/user/weka/"
    return outpath
  }
  
  /**Number of partitions the RDD should have*/
  def getNumberOfPartitions():Int={
    val partitions=Utils.getOption("num-of-partitions",split)
    if(partitions=="") return  4
    return partitions.toInt
  }
  
  /**Number of  Randomized/Stratified RDDs to produce from the dataset*/
  def getNumberOfRandomChunks():Int={
    val chunks=Utils.getOption("num-random-chunks",split)
    if(chunks=="") return 4
    else return chunks.toInt   
  }
  
  /**Number of Attributes in the dataset*/
  def getNumberOfAttributes():Int={
    val atts=Utils.getOption("num-of-attributes",split)
    if(atts=="") return 12
    else return atts.toInt
  }
  
  /**Index of the class attribute*/
  def getClassIndex():Int={
    val index=Utils.getOption("class-index",split)
    if(index=="") return getNumberOfAttributes-1
    else return index.toInt
  }
  
  /**Number of folds in case of cross-validation*/
  def getNumFolds():Int={
    val folds=Utils.getOption("num-folds",split)
    if(folds=="") return 1
    else return folds.toInt
  }
  
  /**Get the attribute names if provided*/
  def getNames():String={
  val names=Utils.getOption("names",split)
  if (names=="") return null
  else return names
  }
  


  /**Get a string that contains the name of the user requested classifier*/
  def getClassifier():String={
    val classifier=Utils.getOption("classifier",split)
    if (classifier=="")return "weka.classifiers.bayes.NaiveBayes"
    else return classifier
  }
  
  /**Get a string that contains the name of the user requestes meta learner */
  def getMetaLearner():String={
    val meta=Utils.getOption("meta",split)
    if (meta=="") return "default"
    return meta
  }
  
  
    /**Return Weka related options (the rest of the options will be returned as well but will be ignored)*/
  def getWekaOptions():Array[String]={
    val wekaOpts=Utils.getOption("weka-options",split)
      // scheme.setOptions(weka.core.Utils.splitOptions("-C 1.0 -L 0.0010 -P 1.0E-12 -N 0 -V -1 -W 1 -K \"weka.classifiers.
       //           functions.supportVector.PolyKernel -C 250007 -E 1.0\""));
    return Utils.splitOptions(wekaOpts)
  } 

  def getParserOptions():Array[String]={
    val parserOpts=Utils.getOption("parser-options",split)
    return Utils.splitOptions(parserOpts)
  }
  }
  