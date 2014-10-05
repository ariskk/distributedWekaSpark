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
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
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
  
  
  
  //String containing user provided options of the format "-option-type1 optionValue1 -option-type2 optionValue2"
  val split=Utils.splitOptions(options)
  val task=Utils.getOption("task",split)
  val dstype=Utils.getOption("dataset-type",split)
  val strategy=Utils.getOption("caching",split)
  val master=Utils.getOption("master",split)
  val hdfsPath=Utils.getOption("hdfs-dataset-path",split)
  val namespath=Utils.getOption("hdfs-names-path",split)
  val headersPath=Utils.getOption("hdfs-headers-path",split)
  val classifierPath=Utils.getOption("hdfs-classifier-path",split) 
  val outpath=Utils.getOption("hdfs-output-path",split)
  val partitions=Utils.getOption("num-of-partitions",split)  
  val chunks=Utils.getOption("num-random-chunks",split)
  val atts=Utils.getOption("num-of-attributes",split)
  val index=Utils.getOption("class-index",split)
  val folds=Utils.getOption("num-folds",split)
  val names=Utils.getOption("names",split)
  val headers=Utils.getOption("gen-header",split)
  val classifier=Utils.getOption("classifier",split)
  val meta=Utils.getOption("meta",split)
  val ruleL=Utils.getOption("rule-learner",split)
  val clust=Utils.getOption("clusterer",split)
  val clusters=Utils.getOption("num-clusters",split)
  val distance=Utils.getOption("distance-metric",split)
  val wekaOpts=Utils.getOption("weka-options",split)
  val parserOpts=Utils.getOption("parser-options",split)
  val compress=Utils.getOption("compress",split)
  val kryo=Utils.getOption("kryo",split)
  val cacheFraction=Utils.getOption("cache-fraction",split)
  val overhead=Utils.getOption("overhead",split)
  val localityDelay=Utils.getOption("locality-wait",split)
  val memory=Utils.getOption("cluster-memory",split)

    
  /**Get a string that describes the user requested task.eg: classification,clustering etc ++*/
  def getTask():String={
    if(task=="") throw new DistributedWekaException("Empty Task Identifier!")
    return task
  }
  
  /**Get a string that describes the type of the dataset to process. Options: Instances, ArrayInstance, ArrayString*/
  def getDatasetType():String={
    if (dstype=="") return "ArrayString"
    return dstype
  }
  
  /**Get the user requested caching strategy*/
  def getCachingStrategy():StorageLevel={
    strategy match {
      case "MEMORY_ONLY" => return StorageLevel.MEMORY_ONLY
      case "MEMORY_AND_DISK" => return StorageLevel.MEMORY_AND_DISK
      case "MEMORY_ONLY_SER" => return StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK_SER" => return StorageLevel.MEMORY_AND_DISK_SER
      case "DISK_ONLY" => return StorageLevel.DISK_ONLY
      case "MEMORY_ONLY_2" => return StorageLevel.MEMORY_ONLY_2
      case "MEMORY_AND_DISK_2" => return StorageLevel.MEMORY_AND_DISK_2
      case "OFF_HEAP" => return StorageLevel.OFF_HEAP
      case _ =>  return null
    }
    
  }
  
  /**Compress the RDDs */
  def useCompression():Boolean={
    if(compress=="y")return true
    return false
  }
  
  /**Use Kryo serialisation*/
  def useKryo():Boolean={
    if(kryo=="y") return true
    return false
  }

  /**Spark Master adress*/
  def getMaster():String={
    if (master==""){println("Master not provided. The tasks will be executed locally.");return "local[*]"}
    else return master
  }
  
  /**HDFS path to the dataset*/
  def getHdfsDatasetInputPath():String={
    if (hdfsPath=="")  throw new Exception("No Path to the dataset was provided!")
    return hdfsPath
  }
  
    /**Returns an hdfs path to the names file*/
  def getNamesPath():String={
    return namespath
  }
  
  /**Returns an hdfs path to the headers file*/
  def getHdfsHeadersInputPath():String={
   return headersPath
  }
  
  /**Returns an hdfs path to a pre-built weka classifier*/
  def getHdfsClassifierInputPath():String={
   return classifierPath
  }
  
  /**Returns an hdfs path to a pre-built weka clusterer*/
  def getHdfsClustererInputPath():String={
    return null
   }
  
  /**Path to save the output */
  def getHdfsOutputPath():String={
    if (outpath=="") return "hdfs://sandbox.hortonworks.com:8020/user/weka/"
    return outpath
  }
  
  /**Number of partitions the RDD should have*/
  def getNumberOfPartitions():Int={
    if(partitions=="") return  0
    return partitions.toInt
  }
  
  /**Number of  Randomized/Stratified RDDs to produce from the dataset*/
  def getNumberOfRandomChunks():Int={
    if(chunks=="") return 0
    else return chunks.toInt   
  }
  
  /**Number of Attributes in the dataset*/
  def getNumberOfAttributes():Int={
    if(atts=="") return 12
    else return atts.toInt
  }
  
  /**Index of the class attribute*/
  def getClassIndex():Int={
    return index.toInt
  }
  
  /**Number of folds in case of cross-validation*/
  def getNumFolds():Int={
    if(folds=="") return 1
    else return folds.toInt
  }
  
  /**Get the attribute names if provided*/
  def getNames():String={
    if (names=="") return null
    return names
  }
  
  /**Whether or not to generate header statistics (may not be needed for numeric attributes)*/
 def generateHeaderStatistics():Boolean={
   if(headers=="y")return true
   return false
 }

  /**Get a string that contains the name of the user requested classifier. Default: NaiveBayes*/
  def getClassifier():String={
    if (classifier=="")return "weka.classifiers.bayes.NaiveBayes"
    else return classifier
  }
  
  /**Get a string that contains the name of the user requested meta-learner */
  def getMetaLearner():String={
    if (meta=="") return "default"
    return meta
  }
  
  /**Get a string that contains the name of the user requested assoc learner*/
  def getRuleLearner():String={
    if (ruleL=="") return "weka.associations.FPGrowth"
    return ruleL
  }
  
  /**Get a string that contains the name of the user requested clusterer*/
  def getClusterer():String={
    if (clust=="") return "weka.clusterers.Canopy"
    return clust
  }
  
  /**Get the user requested number of clusters*/
  def getNumberOfClusters():Int={
    if (clusters=="") return -1
    return clusters.toInt
  }
  
  /**Get the user requested distance metric */
  def getDistanceMetric():String={
    return distance
  }
  
   /**Return Weka related options (the rest of the options will be returned as well but will be ignored)*/
  def getWekaOptions():Array[String]={
    return Utils.splitOptions(wekaOpts)
  } 

  /**Returns the parser options*/
  def getParserOptions():Array[String]={
   return Utils.splitOptions(parserOpts)
  }
  
  /**Heap cache fraction to user */
  def getHeapCacheFraction():Double={
    if(cacheFraction=="")return 0.6
    return cacheFraction.toDouble
  }
  
  /**Overhead of an RDD in comparison with the on-disk raw data */
  def getOverhead():Double={
    if(overhead=="")return 5
    return overhead.toDouble
  }
  
  /**Get the user requested locality delay*/
  def getLocalityDelay:String={
    if(localityDelay=="")return "3000"
    else return localityDelay
  }
  
  /**Cluster-wide main memory (++)*/
  def getClusterMemory:Double={
    if(memory=="")return 0
    else return memory.toDouble
    
  }
  }
  