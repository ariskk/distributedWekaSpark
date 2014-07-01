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
 *    WekaClassifierEvaluationSparkMapper.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierEvaluationMapTask
import weka.classifiers.Classifier
import weka.classifiers.evaluation.Evaluation
import weka.distributed.CSVToARFFHeaderMapTask
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Instance

/**Mapper implementation of the Classifier Evaluation Job
 * 
 * Has Mappers to process datasets in three different formats: Array[String], Array[Instace], Instances
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierEvaluationSparkMapper(headers:Instances,classifier:Classifier,classIndex:Int) extends java.io.Serializable {
  
   //Initialize distributedWekaBase Evaluation map task and csv_rowparser in case the dataset is in String format
   var m_task=new WekaClassifierEvaluationMapTask
   var m_rowparser=new CSVToARFFHeaderMapTask()
   var strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers) //cleanup: this has to be done at task config
   strippedHeaders.setClassIndex(classIndex) // same
   m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
   
   //Set-up map task
   val classAtt=strippedHeaders.classAttribute()
   val seed=1L //random seed
   val classAttSummaryName = CSVToARFFHeaderMapTask.ARFF_SUMMARY_ATTRIBUTE_PREFIX + classAtt.name()
   val summaryClassAtt=headers.attribute(classAttSummaryName)
   m_task.setup(strippedHeaders, computePriors(), computePriorsCount(), seed, 0) //last is predFrac and is used to compute AUC/AuPRC ??
   m_task.setClassifier(classifier)
   m_task.setTotalNumFolds(1)
   
   
   
   /**Map task for evaluation a classifier on a dataset partition (Input in String format)
    * 
    * @param rows is array of Strings (csv format)
    * @return an Evaluation object
    */
   def map (rows:Array[String]): Evaluation={
     for(x <- rows){
       m_task.processInstance(m_rowparser.makeInstance(strippedHeaders, true, m_rowparser.parseRowOnly(x)))
       
     }
     m_task.finalizeTask()
     
     return m_task.getEvaluation()
   }
   
   /**Map task for evaluation a classifier on a dataset partition (Input in Instance format)
    * 
    * @param rows is array of Instance objects
    * @return an Evaluation object
    */
   def map(rows:Array[Instance]): Evaluation={
     for (x <- rows){
       m_task.processInstance(x)
       
     }
     m_task.finalizeTask()
     return m_task.getEvaluation()
    }
   
   
    /**Map task for evaluation a classifier on a dataset partition (Input as an Intances object)
    * 
    * @param instances is a weka Instances object (containes the whole header+dataset for a partition)
    * @return an Evaluation object
    */
    def map(instances:Instances): Evaluation={
     
     //m_task.setInstances(instances)
     m_task.finalizeTask()
     return m_task.getEvaluation()
    }
   

    
    /**Computes priors for each attribute (frequency counts for nominal values or sum of target for numeric)*/
    def computePriors (): Array[Double]={ 
      if(classAtt.isNominal()){
        val priorsNom=new Array[Double](classAtt.numValues())
         for (i <- 0  to classAtt.numValues()-1) {
            val label = classAtt.value(i);
            val labelWithCount = summaryClassAtt.value(i).replace(label + "_", "").trim();
            priorsNom(i) = labelWithCount.toDouble }
            return priorsNom
           }
       else{
         val priorsNonNom=new Array[Double](1)
         priorsNonNom(0)=CSVToARFFHeaderMapTask.ArffSummaryNumericMetric.SUM.valueFromAttribute(summaryClassAtt)
         return priorsNonNom
      }
    
    }
    
    /**Returns the total number of different values seen*/
    def computePriorsCount():Double={
      if(classAtt.isNominal()){return classAtt.numValues()}
      else{ return CSVToARFFHeaderMapTask.ArffSummaryNumericMetric.COUNT.valueFromAttribute(summaryClassAtt)}
      }
    
}