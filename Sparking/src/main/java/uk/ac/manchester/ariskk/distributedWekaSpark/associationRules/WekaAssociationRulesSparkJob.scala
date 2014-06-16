package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import weka.core.Instances
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import weka.core.Instance

/**This job executes a core package associator (FPGrowth and Apriori supported thus far) against a dataset
 * 
 * The association rules mining is performed in 2 phases:
 * Candidate generation phase: Any rule at any partition that meets the global minimum support percentage is considered a candidate
 * Validation phase: The actual support of the candidate rules is computed and the final rules are returned
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaAssociationRulesSparkJob extends java.io.Serializable{
  
  /**Method that processes a dataset in two phases and returns the association rules in HashMap
   * 
   * @param headers are the arff headers of the dataset
   * @param dataset
   * @param minSupport/confidence/lift user thresholds that the generated rules must meet
   * @return a HashMap containing the rules
   */
  def findAssociationRules (headers:Instances,dataset:RDD[String],minSupport:Double,minConfidence:Double,minLift:Double):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.glom.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,null).map(_))
                                    .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))

     val finalRules=dataset.glom.map(new WekaAssociationRulesValidationSparkMapper(headers,null,null).map(_,candidateRules))
                                .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))
   
    return finalRules
  }

    /**Method that processes a dataset in two phases and returns the association rules in HashMap
   * 
   * @param headers are the arff headers of the dataset
   * @param dataset
   * @param minSupport/confidence/lift user thresholds that the generated rules must meet
   * @return a HashMap containing the rules
   */
  def findAssociationRules (headers:Instances,dataset:RDD[Array[Instance]],minSupport:Double,minConfidence:Double,minLift:Double)
                                                                                          (implicit d: DummyImplicit):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,null).map(_))
                                    .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))

     val finalRules=dataset.map(new WekaAssociationRulesValidationSparkMapper(headers,null,null).map(_,candidateRules))
                                .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))
   
    return finalRules
  }
  
      /**Method that processes a dataset in two phases and returns the association rules in HashMap
   * 
   * @param headers are the arff headers of the dataset
   * @param dataset
   * @param minSupport/confidence/lift user thresholds that the generated rules must meet
   * @return a HashMap containing the rules
   */
  def findAssociationRules (headers:Instances,dataset:RDD[Instances],minSupport:Double,minConfidence:Double,minLift:Double)
                                                                          (implicit d1: DummyImplicit, d2:DummyImplicit):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,null).map(_))
                                    .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))

     val finalRules=dataset.map(new WekaAssociationRulesValidationSparkMapper(headers,null,null).map(_,candidateRules))
                                .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))
   
    return finalRules
  }
}