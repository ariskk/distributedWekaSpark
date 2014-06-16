package uk.ac.manchester.ariskk.distributedWekaSpark.main


import java.util.ArrayList
import weka.distributed.DistributedWekaException
import weka.core.Utils
import weka.distributed.DistributedWekaException

/**Class that parses the String of user-provided options and has methods to return ind
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class OptionsParser (options:String) {
  
  //String containing user provided options of the format "-option-type1 optionValue1 -option-type2 optionValue2"
  val split=Utils.splitOptions(options)
  
  /**Get a string that describes the user requested task.eg: classification,clustering etc ++*/
  def getTask():String={
    val task=Utils.getOption("task",split)
    if(task=="") throw new DistributedWekaException("Unrecognised Task Identifier!")
    return task
  }
  
  /**Get a string that describes the type of the dataset to process. Options: Instances, ArrayInstance, ArrayString*/
  def getDatasetType():String={
    val dstype=Utils.getOption("dataset-type",split)
    if (dstype=="") return "Instances"
    else return dstype
  }
  
  //will be removed
  def getDep():Int={
   return  Utils.getOption("depth",split).toInt
  }
  
  /**Return Weka related options (the rest of the options will be returned as well but will be ignored)*/
  def getWekaOptions():Array[String]={
    val wekaOpts=Utils.getOption("weka-options",split)
      // scheme.setOptions(weka.core.Utils.splitOptions("-C 1.0 -L 0.0010 -P 1.0E-12 -N 0 -V -1 -W 1 -K \"weka.classifiers.
       //           functions.supportVector.PolyKernel -C 250007 -E 1.0\""));
    return Utils.splitOptions(wekaOpts)
  } 
  
  /**Spark Master adress*/
  def getMaster():String={
    val master=Utils.getOption("master",split)
    if (master==""){println("Master not provided. The tasks will be executed locally.");return "local[4]"}
    else return master
  }
  
  /**HDFS path to the dataset*/
  def getHdfsDatasetInputPath():String={
    val hdfsPath=Utils.getOption("hdfs-input-path",split)
    if (hdfsPath=="")  return "hdfs://sandbox.hortonworks.com:8020/user/weka/record1.csv"
    else return hdfsPath
  }
  def getHdfsHeadersInputPath():String={
   val headersPath=Utils.getOption("-hdfs-headers-path",split)
   return headersPath
    
  }
  def getHdfsClassifierInputPath():String={
     val classifierPath=Utils.getOption("-hdfs-classifier-path",split) 
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
    val partitions=Utils.getOption("num-partitions",split)
    if(partitions=="")return  4
    else return partitions.toInt
  }
  
  /**Number of  Randomized/Stratified RDDs to produce from the dataset*/
  def getNumberOfRandomChunks():Int={
    val chunks=Utils.getOption("num-randChunks",split)
    if(chunks=="") return 4
    else return chunks.toInt   
  }
  
  /**Number of Attributes in the dataset*/
  def getNumberOfAttributes():Int={
    val atts=Utils.getOption("num-ofatts",split)
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
    if(folds=="") return 4
    else return folds.toInt
  }
  
  /**Get the attribute names if provided*/
  def getNames():Array[String]={
  val names=Utils.getOption("names",split)
  if (names=="") return null
  else return names.split(",")
  }
  

  /**Returns an hdfs path to the names file*/
  def getNamesPath():String={
    val namespath=Utils.getOption("names-path",split)
    if (namespath=="") return "hdfs://sandbox.hortonworks.com:8020/user/weka/namessupermarket"
    else return namespath
  }
  
  /**Get a string that contains the name of the user requested classifier*/
  def getClassifier():String={
    val classifier=Utils.getOption("classifier",split)
    if (classifier=="")return "weka.classifiers.functions.VotedPerceptron"
    else return classifier
  }
  
  /**Get a string that contains the name of the user requestes meta learner */
  def getMetaLearner():String={
    val meta=Utils.getOption("meta",split)
    return meta
  }

  }
  