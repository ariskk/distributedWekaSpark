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
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
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



/** Project main  
 *  
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   
 *   ToDo: user-interface, option parser and loader-saver for persistence  */


object distributedWekaSpark {
   def main(args : Array[String]){
       
      val optionsHandler=new OptionsParser(args.mkString(" "))
      
      
     
      ///Input Parameters . ToDo: accept params as args(0), args(1) etc from command line , 
      val master=optionsHandler.getMaster
      val hdfsPath=optionsHandler.getHdfsPath
      val numberOfPartitions=optionsHandler.getNumberOfPartitions
      val numberOfAttributes=optionsHandler.getNumberOfAttributes
      val classifierToTrain=optionsHandler.getClassifier //this must done in-Weka somehow
      val metaL="default"  //default is weka.classifiers.meta.Vote
      val classAtt=optionsHandler.getClassIndex
      val randomChunks=optionsHandler.getNumberOfRandomChunks
      val names=new ArrayList[String]
      val folds=optionsHandler.getNumFolds
      val headerJobOptions=null
      
      
      
       
      //Configuration of Context - need to check that at a large scale: spark seems to add a context by default
      val conf=new SparkConf().setAppName("distributedWekaSpark").setMaster(optionsHandler.getMaster).set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      val hdfshandler=new HDFSHandler(sc)
     
      
      
     // System.exit(0)
      
      //Load Dataset and cache. ToDo: global caching strategy   -data.persist(StorageLevel.MEMORY_AND_DISK)
       var dataset=hdfshandler.loadFromHDFS(hdfsPath, numberOfPartitions)
       dataset.cache()
       //glom? here on not?
       
       
       //headers
         val headerjob=new CSVToArffHeaderSparkJob
         val headers=headerjob.buildHeaders(headerJobOptions,names,numberOfAttributes,dataset)
      // hdfshandler.saveToHDFS(headers, "user/weka/testhdfs.txt", "testtext")
        
        // System.exit(0)
       //randomize if necessary 
 //      if(randomChunks>0){dataset=new WekaRandomizedChunksSparkJob().randomize(dataset, randomChunks, headers, classAtt)}
       
     //build foldbased
//      val foldjob=new WekaClassifierFoldBasedSparkJob
//      val classifier=foldjob.buildFoldBasedModel(dataset, headers, folds, classifierToTrain, metaL,classAtt)
//      println(classifier.toString())
//      val evalfoldjob=new WekaClassifierEvaluationSparkJob
//      val eval=evalfoldjob.evaluateFoldBasedClassifier(folds, classifier, headers, dataset,classAtt)
//      evalfoldjob.displayEval(eval)
//      
//      //build a classifier+ evaluate
//      val classifierjob=new WekaClassifierSparkJob
//      val classifier2=classifierjob.buildClassifier(metaL,classifierToTrain,classAtt,headers,dataset,null,optionsHandler.getWekaOptions) 
//      val evaluationJob=new WekaClassifierEvaluationSparkJob
//      val eval2=evaluationJob.evaluateClassifier(classifier2, headers, dataset,classAtt)
//
//      println(classifier2.toString())
//      evaluationJob.displayEval(eval2)
    
      
      val rulejob=new WekaAssociationRulesSparkJob
      val rules=rulejob.findAssociationRules(headers, dataset, 0.1, 1, 1)
      rules.foreach{
        keyv => println(keyv._2.getRuleString)
        
      }
   }
   
     
}