package uk.ac.man.aris

import weka.core.Instances
import weka.classifiers.Classifier
import weka.distributed.WekaClassifierMapTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask._
import weka.distributed.CSVToARFFHeaderReduceTask

/**Mapper implementation for WekaClassifierSpark job 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *  
 *  Trains and returns a classifier on a dataset partition  */

class WekaClassifierSparkMapper (classifierOptions:Array[String],rowparserOptions:Array[String],classifier:String,header:Instances) extends java.io.Serializable{
  var strippedHeader:Instances=null
  var m_task=new WekaClassifierMapTask()
  var m_rowparser=new CSVToARFFHeaderMapTask()
  
  
  //true in make instance means classifier is updateable
  def map(rows:Array[String]): Classifier={
    this.setupTask
    for(x <- rows){
      m_task.processInstance(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x))) //many options here: updatable/not, batch/not, forced must do++
                    }
    m_task.finalizeTask()
    return m_task.getClassifier()    //he also saves number of instances (for voting) in the same file. must check reducer
  }

   def setupTask() :Unit={
     m_task.setOptions(classifierOptions)
     m_task.setClassifier(null)
     m_rowparser.setOptions(rowparserOptions)
     strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(header)
     strippedHeader.setClassIndex(11)
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
     m_task.setClassifier(new weka.classifiers.bayes.NaiveBayes)
     m_task.setup(strippedHeader)
   }
}