package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierEvaluationMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.core.Instances
import java.util.ArrayList
import weka.classifiers.Classifier
import weka.classifiers.evaluation.Evaluation
import weka.distributed.WekaClassifierEvaluationReduceTask
/**Mapper taks for fold-based evaluation
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedEvaluationSparkMapper(headers:Instances,classifier:Classifier,folds:Int,classIndex:Int) extends java.io.Serializable {

   //ToDo: isupdatable, forced trained, documentation ++
  
   
   var m_combiner=new WekaClassifierEvaluationReduceTask ////is this?
   
   //Initialize a container for the WekaBase tasks
   var m_tasks=new ArrayList[WekaClassifierEvaluationMapTask]
   
   //Initialize a csv row parser and remove statistics from the headers and set the class attribute
   var m_rowparser=new CSVToARFFHeaderMapTask()
   var strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
   strippedHeaders.setClassIndex(classIndex) //ToDo:must be provided in the constructor
   m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
   val classAtt=strippedHeaders.classAttribute()
   
   //Initialize one WekaBase Map task for each fold
   val seed=1L
   val classAttSummaryName = CSVToARFFHeaderMapTask.ARFF_SUMMARY_ATTRIBUTE_PREFIX + classAtt.name()
   val summaryClassAtt=headers.attribute(classAttSummaryName)
   for(i<-0 to folds-1){
   m_tasks.add(new WekaClassifierEvaluationMapTask)
   m_tasks.get(i).setClassifier(classifier)
   m_tasks.get(i).setFoldNumber(i+1)
   m_tasks.get(i).setTotalNumFolds(folds)
   //Compute priors for the evaluation tasks
   m_tasks.get(i).setup(strippedHeaders, computePriors(), computePriorsCount(), seed, 0) //last is predFrac and is used to compute AUC/AuPRC ?? setbatch trained incremental ??
   }
  
   
  /**Fold-based Evaluation Mapper
   * 
   * @param rows represent the dataset partition
   * @return Evaluation is the fold-based evaluation model computed per partition
   */
  def map(rows:Array[String]): Evaluation={
   val evals=new ArrayList[Evaluation]
   for(i<-0 to rows.length-1){
     for(j<-0 to folds-1){
       //m_task checks if instance is in the fold set. no need to check here
      
      m_tasks.get(j).processInstance(m_rowparser.makeInstance(strippedHeaders, true, m_rowparser.parseRowOnly(rows(i))))
      }
    }
    for(j<-0 to folds-1){
      m_tasks.get(j).finalizeTask()
      evals.add(m_tasks.get(j).getEvaluation())
    }
    return m_combiner.aggregate(evals)   //needs semantic checking
  }
   
  
  /**Compute the attribute Priors for nominal and non nominal values
   * 
   * @return an array of Doubles (one per attribute)
   */
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
    
  /**Computes prior counts (number of differet values)
   * 
   * @return a prior count
   */
  def computePriorsCount():Double={
      if(classAtt.isNominal()){return classAtt.numValues()}
      else{ return CSVToARFFHeaderMapTask.ArffSummaryNumericMetric.COUNT.valueFromAttribute(summaryClassAtt)}
      }
    
}