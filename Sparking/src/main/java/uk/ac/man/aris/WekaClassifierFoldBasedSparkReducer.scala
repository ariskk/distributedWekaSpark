package uk.ac.man.aris

import weka.classifiers.evaluation.Evaluation
import weka.classifiers.Classifier
import java.util.ArrayList
import weka.distributed.WekaClassifierReduceTask



/**Spark Reducer for training an arbitrary number of folds
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedSparkReducer(folds:Int) extends java.io.Serializable{
  
  var r_task=new WekaClassifierReduceTask
  
  
  def reduce(modelsA:ArrayList[Classifier],modelsB:ArrayList[Classifier]): ArrayList[Classifier]={ 
    var toaggregate_models=new ArrayList[ArrayList[Classifier]]
    var aggregated_models=new ArrayList[Classifier]
    for(i<-0 to folds-1){
    toaggregate_models.add(new ArrayList)
    toaggregate_models.get(i).add(modelsA.get(i))  
    toaggregate_models.get(i).add(modelsB.get(i)) 
    }
    for(i<-0 to folds-1){
    aggregated_models.add(r_task.aggregate(toaggregate_models.get(i))) 
    }
    return aggregated_models}
  
  
}