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
 *    WekaClassifierSparkMapper.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.core.Instances
import weka.core.Utils._
import weka.classifiers.Classifier
import weka.distributed.WekaClassifierMapTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask._
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.classifiers.SingleClassifierEnhancer
import weka.core.Utils
import weka.core.Instance
import weka.classifiers.meta.FilteredClassifier

/**Mapper implementation for WekaClassifierSpark job 
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *  
 *  Trains and returns a classifier on a dataset partition 
 *  @param classAtt is the class index
 *  @param metaLearner is the requested metaLearner. if 'default' Vote will be used
 *  @param classifierToTrain is the requested base classifier
 *  @param two option strings for parser/classifier
 *  @param header is the header file for the job */
class WekaClassifierSparkMapper (metaLearner:String,classifierToTrain:String,classifierOptions:Array[String],
                                  rowparserOptions:Array[String],header:Instances) extends java.io.Serializable{

    //Initialize the parser and the Base Map task(It processes a set of instances and produces a classifier)
    var m_task=new WekaClassifierMapTask()
    var m_rowparser=new CSVToARFFHeaderMapTask()
    m_task.setOptions(classifierOptions)
    m_rowparser.setOptions(rowparserOptions)
   
    //Set the classifier to train 
    val obj=Class.forName(classifierToTrain).newInstance()
    val cla=obj.asInstanceOf[Classifier]
    

   
    //Check if a custom MetaLearner is requested
    if(metaLearner!="default"){
	    val obj2=Class.forName(metaLearner).newInstance()
	    val claMeta=obj2.asInstanceOf[SingleClassifierEnhancer]
	    claMeta.setClassifier(cla)
        m_task.setClassifier(claMeta)
    }
    else{
        m_task.setClassifier(cla) 
    }

  
    //Remove the summary from the headers. Set the class attribute
    val strippedHeader:Instances=CSVToARFFHeaderReduceTask.stripSummaryAtts(header) //clean-up headers should be set at this point
    //strippedHeader.setClassIndex(classIndex) //clean-up
    m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
    m_task.setup(strippedHeader)
  
  
  
  //true in makeInstance means classifier is updateable
  /**Map task for training a classifier using an Array[String] (String represents a line of a  csv file)
   * 
   * @param rows is a dataset partition in Array[String] format
   * @return a trained classifier on the provided partition
   */
   def map(rows:Array[String]): Classifier={
     for(x <- rows){     
       m_task.processInstance(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x)))
       }                                    //ToDo:many options here: updatable/not, batch/not, forced
       m_task.finalizeTask()
    return m_task.getClassifier()        //he also saves number of instances (for voting) in the same file. must check reducer
   } 
   
 /**Map task for training classifiers using an Array[Instance]
   * 
   * @param rows is a dataset partition in Array[Instance] format
   * @return a trained classifier on the provided parition
   */
   def map(rows:Array[Instance]):Classifier={
     for(x<-rows){
       m_task.processInstance(x)
       }
     m_task.finalizeTask
     return m_task.getClassifier()
   }
   
  /**Map task for training classifiers using an Instances object
   * 
   * @param instances is a dataset partition as
   * @return a trained classifier on the provided parition
   */
   def map(instances:Instances):Classifier={
     //m_task.setInstances(instances) Not yet supported by the Base tasks
     m_task.finalizeTask
     return m_task.getClassifier()
   }
   
 
}