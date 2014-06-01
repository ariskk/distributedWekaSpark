package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierEvaluationMapTask
import weka.classifiers.Classifier
import weka.classifiers.evaluation.Evaluation
import weka.distributed.CSVToARFFHeaderMapTask
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask

class WekaClassifierEvaluationSparkMapper(headers:Instances,classifier:Classifier) extends java.io.Serializable {
  
   var m_task=new WekaClassifierEvaluationMapTask
   var m_rowparser=new CSVToARFFHeaderMapTask()
   var strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
   strippedHeaders.setClassIndex(11)
   m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
   val classAtt=strippedHeaders.classAttribute()
   val seed=1L
   val classAttSummaryName = CSVToARFFHeaderMapTask.ARFF_SUMMARY_ATTRIBUTE_PREFIX + classAtt.name()
   val summaryClassAtt=headers.attribute(classAttSummaryName)
   m_task.setup(strippedHeaders, computePriors(), computePriorsCount(), seed, 0) //last is predFrac and is used to compute AUC/AuPRC ??
   m_task.setClassifier(classifier)
   m_task.setTotalNumFolds(1)
   
   def map (rows:Array[String]): Evaluation={
     for(x <- rows){
       m_task.processInstance(m_rowparser.makeInstance(strippedHeaders, true, m_rowparser.parseRowOnly(x)))
       
     }
     m_task.finalizeTask()
     
     return m_task.getEvaluation()
   }

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
    
    
    def computePriorsCount():Double={
      if(classAtt.isNominal()){return classAtt.numValues()}
      else{ return CSVToARFFHeaderMapTask.ArffSummaryNumericMetric.COUNT.valueFromAttribute(summaryClassAtt)}
      }
    
}