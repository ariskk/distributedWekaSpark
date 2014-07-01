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
 *    WekaClassifierFoldBasedSparkMapper.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierMapTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Instances
import weka.classifiers.Classifier
import weka.classifiers.SingleClassifierEnhancer
import java.util.ArrayList
import weka.core.Instance


/**Spark Mapper for training an arbitrary number of folds
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedSparkMapper(folds:Int,headers:Instances,toTrain:String,metaLearner:String,classIndex:Int) extends java.io.Serializable{
  
    //Initialize a Base Map Task for each fold
    val m_tasks=new ArrayList[WekaClassifierMapTask]
    for(i<-0 to folds-1){
      m_tasks.add(new WekaClassifierMapTask())
    }
    
    //ToDo:check if it is updatable or incremental and forced
    
    //Initialize a csv rowparser. Remove summary stats from the headers and set class index
    var m_rowparser=new CSVToARFFHeaderMapTask()
    var strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
    strippedHeaders.setClassIndex(classIndex) 
    m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
  
  
    //Set the class to train. Set a custom Meta-Learner if requested else leave default 
    val obj=Class.forName(toTrain).newInstance()
    val cla=obj.asInstanceOf[Classifier]
  
    //Setup the Base Map task (one per fold)
    for(i<-0 to folds-1){
       m_tasks.get(i).setFoldNumber(i+1)
       m_tasks.get(i).setTotalNumFolds(folds)
       //ToDo: setContinueTrainingUpdateble , addPreconstructedFilter
   
       if(metaLearner!="default"){
	     val obj2=Class.forName(metaLearner).newInstance()
	     val claMeta=obj2.asInstanceOf[SingleClassifierEnhancer]
	     claMeta.setClassifier(cla)
         m_tasks.get(i).setClassifier(claMeta)
        }
       else{
         m_tasks.get(i).setClassifier(cla) 
       }
       m_tasks.get(i).setup(strippedHeaders)
     }
   
   
   
   
  /**Mapper task for fold-based classifier training. Accepts dataset as an Array[String]. Each String represents a line from the csv file
   * 
   * trains a classifier for each fold and returns an ArrayList of classifiers
   * @param rows are the rows of a dataset partition
   * @return an ArrayList of classifiers
   */ 
  def map(rows:Array[String]): ArrayList[Classifier]={
    val models=new ArrayList[Classifier]
    for(i<-0 to rows.length-1){
      //m_task checks if instance is in the fold set. No need to check here
      m_tasks.get(i%folds).processInstance(m_rowparser.makeInstance(strippedHeaders, true, m_rowparser.parseRowOnly(rows(i))))
    }
    for(j<-0 to folds-1){
      m_tasks.get(j).finalizeTask()
      models.add(m_tasks.get(j).getClassifier())
    }
    return models
  }

   /**Mapper task for fold-based classifier training. Accepts dataset as an Array[Instance]
   * 
   * trains a classifier for each fold and returns an ArrayList of classifiers
   * @param rows are the rows of a dataset partition
   * @return an ArrayList of classifiers
   */ 
  def map(rows:Array[Instance]): ArrayList[Classifier]={
    val models=new ArrayList[Classifier]
    for(i<-0 to rows.length-1){
      //m_task checks if instance is in the fold set. No need to check here
      m_tasks.get(i%folds).processInstance(rows(i))
    }
    for(j<-0 to folds-1){
      m_tasks.get(j).finalizeTask()
      models.add(m_tasks.get(j).getClassifier())
    }
    return models
  }
  
    /**Mapper task for fold-based classifier training. Accepts dataset as an Instances object
   * 
   * trains a classifier for each fold and returns an ArrayList of classifiers
   * @param rows are the rows of a dataset partition
   * @return an ArrayList of classifiers
   */ 
  def map(instances:Instances): ArrayList[Classifier]={
    val models=new ArrayList[Classifier]
   
     //m_task.setInstaces(instances)
    for(j<-0 to folds-1){
      m_tasks.get(j).finalizeTask()
      models.add(m_tasks.get(j).getClassifier())
    }
    return models
  }
}