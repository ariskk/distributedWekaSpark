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
 *    distributedWekaSpark.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */


package uk.ac.manchester.ariskk.distributedWekaSpark.main

import java.util.ArrayList
import weka.core.Utils
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierEvaluationSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierFoldBasedSparkJob
import java.io.DataOutput
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import weka.associations.AssociationRule
import weka.associations.AssociationRulesProducer
import weka.associations.Apriori
import org.apache.spark.rdd.RDD
import weka.core.Instance
import uk.ac.manchester.ariskk.distributedWekaSpark.associationRules.WekaAssociationRulesSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.associationRules.UpdatableRule
import java.util.Collections
import java.util.Comparator
import scala.util.Sorting
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import weka.classifiers.trees.J48
import uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs.WekaInstancesRDDBuilder
import weka.clusterers.Canopy
import uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs.WekaInstanceArrayRDDBuilder
import uk.ac.manchester.ariskk.distributedWekaSpark.classifiers.WekaClassifierSparkJob
import weka.classifiers.bayes.NaiveBayes
import weka.classifiers.bayes.BayesNet
import weka.classifiers.functions.SGD
import weka.classifiers.trees.RandomTree
import weka.classifiers.rules.DecisionTable
import org.apache.spark.storage.StorageLevel
import weka.clusterers.SimpleKMeans
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkMapper
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkReducer
import com.esotericsoftware.minlog.Log
import com.esotericsoftware.minlog.Log._
import weka.clusterers._
import weka.classifiers.Evaluation
import weka.classifiers.evaluation.AggregateableEvaluation






/** Project main  
 *  
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   
 *   ToDo: user-interface, option parser and loader-saver for persistence  
 *   */
object distributedWekaSpark extends java.io.Serializable{
   def main(args : Array[String]){
     
      println(args.mkString(" "))
      val options=new OptionsParser(args.mkString(" "))
     // Log.set(LEVEL_TRACE)

      val conf=new SparkConf().setAppName("distributedWekaSpark")
      
     conf.set("spark.locality.wait",options.getLocalityDelay)
      
      
      if(options.useCompression){
    	  conf.set("spark.rdd.compress","true")
      }
      
      if(options.useKryo){
    	  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    	  conf.set("spark.kryo.registrator", "uk.ac.manchester.ariskk.distributedWekaSpark.kryo.WekaSparkKryoRegistrator")
      }
      
      if(options.getHeapCacheFraction!=""){
    	  conf.set("spark.storage.memoryFraction",options.getHeapCacheFraction.toString)
      }
      
      var sc=new SparkContext(conf)
     
      
      
      val hdfshandler=new HDFSHandler(sc)
      val utils=new wekaSparkUtils
      
      //++
      val slaveMemoryMap=sc.getExecutorMemoryStatus
      var totalMem=0L
      for(x <- slaveMemoryMap){ 
        totalMem=totalMem+x._2._2
       }
      
      
      println("*****************************"+totalMem)
      
      if(options.getCachingStrategy==null){
        val cachingSelector=new CachingStrategySelector(hdfshandler.getFileSize(options.getHdfsDatasetInputPath),
                                                        options.getClusterMemory*options.getHeapCacheFraction,options.getOverhead)
        if(cachingSelector.getUseKryo){
          sc.setLocalProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          sc.setLocalProperty("spark.kryo.registrator", "uk.ac.manchester.ariskk.distributedWekaSpark.kryo.WekaSparkKryoRegistrator")
          println("Using Kryo")
         }
         if(cachingSelector.getUseCompression){
          sc.setLocalProperty("spark.rdd.compress","true")
          println("Using Compression")
         }
         println("******")
         println("Using "+cachingSelector.getStorageLevel.description)
         println("******")
        
         val task=new TaskExecutor(hdfshandler,options,cachingSelector.getStorageLevel)
      }
      else{
        val task=new TaskExecutor(hdfshandler,options,options.getCachingStrategy)
      }
      

      
      
   }
   
     
}