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
import weka.classifiers.evaluation.Evaluation
import weka.classifiers.bayes.NaiveBayes
import weka.classifiers.bayes.BayesNet
import weka.classifiers.functions.SGD
import weka.classifiers.trees.RandomTree
import weka.classifiers.rules.DecisionTable
import org.apache.spark.storage.StorageLevel




/** Project main  
 *  
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   
 *   ToDo: user-interface, option parser and loader-saver for persistence  */


object distributedWekaSpark extends java.io.Serializable{
   def main(args : Array[String]){
       
      println(args.mkString(" "))
        val options=new OptionsParser(args.mkString(" "))
      

      
      //Configuration of Context - need to check that at a large scale: spark seems to add a context by default
      val conf=new SparkConf().setAppName("distributedWekaSpark").setMaster(options.getMaster).set("spark.executor.memory","1g")
      val sc=new SparkContext(conf)
      val hdfshandler=new HDFSHandler(sc)
      val utils=new wekaSparkUtils
      //var data=hdfshandler.loadRDDFromHDFS(options.getHdfsDatasetInputPath, options.getNumberOfPartitions)
     // data.persist(options.getCachingStrategy)
      //convert dataset either here or in Task.config
      val task=new TaskExecutor(hdfshandler,options)
      
      exit(0)
     
   
     //Dummy test-suite
     
      
      
     
      ///Input Parameters 
      val master=options.getMaster
      val hdfsPath=options.getHdfsDatasetInputPath
      val numberOfPartitions=options.getNumberOfPartitions
      val numberOfAttributes=options.getNumberOfAttributes
      val classifierToTrain=options.getClassifier //this must done in-Weka somehow
      val metaL=options.getMetaLearner  //default is weka.classifiers.meta.Vote
      val classAtt=options.getClassIndex
      val randomChunks=options.getNumberOfRandomChunks
      var names=new ArrayList[String]
      val folds=options.getNumFolds
      val headerJobOptions=Utils.splitOptions("-N first-last")
       
      val namesPath=options.getNamesPath
      
      
      
     
     
      
      

      
      //Load Dataset and cache. ToDo: global caching strategy   -data.persist(StorageLevel.MEMORY_AND_DISK)
       var dataset=hdfshandler.loadRDDFromHDFS(hdfsPath, numberOfPartitions)
       var dataset2=hdfshandler.loadRDDFromHDFS(hdfsPath, 1)
       dataset.persist(options.getCachingStrategy)
       
       
       //glom? here on not?
       val namesfromfile=hdfshandler.loadRDDFromHDFS(namesPath,1)
       println(namesfromfile.collect.mkString(""))
   
       
        val names1=new ArrayList[String];for (i <- 0 to 11){names1.add("at"+i)}   
        names=utils.getNamesFromString(namesfromfile.collect.mkString(""))
      //  val names1=utils.getNamesFromString(optionsHandler.getNames.mkString(""))
       //headers
        
        val headerjob=new CSVToArffHeaderSparkJob
        val headers=headerjob.buildHeaders(headerJobOptions,names,numberOfAttributes,dataset)
       // headers.setClassIndex(11)

        println(headers)
     

        
        
        
       
       var m_rowparser=new CSVToARFFHeaderMapTask()
       var dat=dataset.glom.map(new WekaInstancesRDDBuilder().map(_,headers))
       //var dat3=dataset2.glom.map(new WekaInstancesRDDBuilder().map(_,headers))
       var dat2=dataset.glom.map(new WekaInstanceArrayRDDBuilder().map(_,headers))
       dat2.cache
       
       
       
       
      val rulejob=new WekaAssociationRulesSparkJob
      val rules=rulejob.findAssociationRules(dataset,headers,  0.1, 1, 1)
      val rulesA=rulejob.findAssociationRules(dat2,headers,  0.1, 1, 1)
      val rulesB=rulejob.findAssociationRules(dat,headers, 0.1, 1, 1)
      val array=new Array[UpdatableRule](rules.keys.size)
      var j=0
      rules.foreach{ 
        keyv => 

          array(j)=keyv._2
        j+=1
       }
       Sorting.quickSort(array)
       val fullsupport=new Array[String](array.length)
       val lesssupport=new Array[String](array.length)
       var i=0;var o=0;
       array.foreach{x =>
         x.getTransactions match{
           case  n if n>3000 => fullsupport(i)=x.getRuleString;i+=1
           case _ =>   lesssupport(o)=x.getRuleString;o+=1
         }}
        println("\n Full Support \n")
        fullsupport.foreach{x => if(x!=null)println(x)} 
        println("\n Less support \n")
        lesssupport.foreach{x => if(x!=null)println(x)}
          exit(0)
//   
//   exit(0)
//       
//       val classifierfold=new WekaClassifierFoldBasedSparkJob
//       val classf1=classifierfold.buildFoldBasedModel(dataset, headers, folds, classifierToTrain, "default", 11)
//       println("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
//       val classf2=classifierfold.buildFoldBasedModel(dat2, headers, folds, classifierToTrain, "default", 11)
//       println("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
      // val classf3=classifierfold.buildFoldBasedModel(dat, headers, folds, classifierToTrain, "default", 11)
      // println(classf1)
      // println(classf2)
     //  println(classf3)
//       val evalJ1=new WekaClassifierEvaluationSparkJob
//       val e11=evalJ1.evaluateClassifier(classf1, headers, dataset, 11)
//       val e21=evalJ1.evaluateClassifier(classf2, headers, dat2, 11)
//       evalJ1.displayEval(e11)
//       evalJ1.displayEval(e21)
//       
//       exit(0)
       
       val classifierj=new WekaClassifierSparkJob
       
      // val classu=classifierj.buildClassifier(dat,"weka.classifiers.meta.Bagging", classifierToTrain,  headers,  null, null)
       val classu1=classifierj.buildClassifier(dat2,"default", classifierToTrain,  headers,  null, null)
       val classu2=classifierj.buildClassifier(dataset, "default", classifierToTrain, headers,  null, null)
      // println(classu)
       println(classu1)
       println(classu2)
       
       val evalJ=new WekaClassifierEvaluationSparkJob
       val e1=evalJ.evaluateClassifier(classu1, headers, dataset, 11)
       val e2=evalJ.evaluateClassifier(classu2, headers, dat2, 11)
     //  val e3=evalJ.evaluateClassifier(classu, headers, dat, 11)
       println(evalJ.displayEval(e1))
       println(evalJ.displayEval(e2))
      // println(evalJ.displayEval(e3))
       val ttest=new NaiveBayes
      // ttest.buildClassifier(dat3.first)
       
     //  val e4=new Evaluation(dat3.first)
      // e4.evaluateModel(x$1, x$2, x$3)
     //  e4.evaluateModel(ttest, dat3.first, null)
      // evalJ.displayEval(e4)
       
       
       exit(0)
       
       
      // var classifier=dat.map(x=> new TestClassifierMapper().map(x)).collect
      // classifier.foreach{x => println(x)}
     
       
       
    // exit(0)
      // hdfshandler.saveToHDFS(headers, "user/weka/testhdfs.txt", "testtext")
       
        // hdfshandler.saveObjectToHDFS(headers, "hdfs://sandbox.hortonworks.com:8020/user/weka/", null)
      //   val h=hdfshandler.loadObjectFromHDFS("hdfs://sandbox.hortonworks.com:8020/user/weka/")
        // val h2=new Instances(h)

         
       //randomize if necessary 
      // if(randomChunks>0){dataset=new WekaRandomizedChunksSparkJob().randomize(dataset, randomChunks, headers, classAtt)}
       
     //build foldbased
//      val foldjob=new WekaClassifierFoldBasedSparkJob
//      val classifier=foldjob.buildFoldBasedModel(dataset, headers, folds, classifierToTrain, metaL,classAtt)
//      println(classifier.toString())
//      val evalfoldjob=new WekaClassifierEvaluationSparkJob
//      val eval=evalfoldjob.evaluateFoldBasedClassifier(folds, classifier, headers, dataset,classAtt)
//      evalfoldjob.displayEval(eval)
  
//      //build a classifier+ evaluate
      val classifierjob=new WekaClassifierSparkJob
      val classifier2=classifierjob.buildClassifier(dataset,metaL,classifierToTrain,headers,null,options.getWekaOptions) 
      println(classifier2)
//      val evaluationJob=new WekaClassifierEvaluationSparkJob
//      val eval2=evaluationJob.evaluateClassifier(classifier2, headers, dataset,classAtt)
//
//      println(classifier2.toString())
//      evaluationJob.displayEval(eval2)
    
      //val broad=sc.broadcast(headers)
      
      exit(0)
      
      
   }
   
     
}