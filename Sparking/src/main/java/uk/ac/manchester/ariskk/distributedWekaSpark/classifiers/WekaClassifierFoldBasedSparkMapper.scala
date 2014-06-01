package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers
import weka.distributed.WekaClassifierMapTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Instances
import weka.classifiers.Classifier
import weka.classifiers.SingleClassifierEnhancer
import java.util.ArrayList


/**Spark Mapper for training an arbitrary number of folds
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedSparkMapper(folds:Int,headers:Instances,toTrain:String,metaLearner:String) extends java.io.Serializable{
  
  //ToDo:parse string for folds not explicit
  val m_tasks=new ArrayList[WekaClassifierMapTask]
  for(i<-0 to folds-1){
    m_tasks.add(new WekaClassifierMapTask())
  }
  //ToDo:check if it is updatable or incremental and forced
   var m_rowparser=new CSVToARFFHeaderMapTask()
   var strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
   strippedHeaders.setClassIndex(11) //must be passed 
   m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
   val classAtt=strippedHeaders.classAttribute()
     //set the class to train. set a custom Meta-Learner if requested else leave default 
  val obj=Class.forName(toTrain).newInstance()
  val cla=obj.asInstanceOf[Classifier]
  
  //setup the tasks (one per fold)
   for(i<-0 to folds-1){
     m_tasks.get(i).setFoldNumber(i+1)
     m_tasks.get(i).setTotalNumFolds(folds)
     //setContinueTrainingUpdateble , addPreconstructedFilter
   
      if(metaLearner!="default"){
	   val obj2=Class.forName(metaLearner).newInstance()
	   val claMeta=obj2.asInstanceOf[SingleClassifierEnhancer]
	   claMeta.setClassifier(cla)
       m_tasks.get(i).setClassifier(claMeta)
      }
      else{
       m_tasks.get(i).setClassifier(cla) 
       }
       //set-up a string as a key for reducer
       m_tasks.get(i).setup(strippedHeaders)
   }
   
   
   
   
   
  def map(rows:Array[String]): ArrayList[Classifier]={
    val models=new ArrayList[Classifier]
    for(i<-0 to rows.length-1){
      //m_task checks if instance is in the fold set. no need to check here
    m_tasks.get(i%folds).processInstance(m_rowparser.makeInstance(strippedHeaders, true, m_rowparser.parseRowOnly(rows(i))))}
    for(j<-0 to folds-1){
    m_tasks.get(j).finalizeTask()
    models.add(m_tasks.get(j).getClassifier())
    }
    return models
  }

}