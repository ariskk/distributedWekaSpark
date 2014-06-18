package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import weka.core.Instances
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import weka.core.Instance
import scala.util.Sorting

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
  def findAssociationRules (dataset:RDD[String],headers:Instances,minSupport:Double,minConfidence:Double,minLift:Double):HashMap[String,UpdatableRule]={
    
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
  def findAssociationRules (dataset:RDD[Array[Instance]],headers:Instances,minSupport:Double,minConfidence:Double,minLift:Double)
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
  def findAssociationRules (dataset:RDD[Instances],headers:Instances,minSupport:Double,minConfidence:Double,minLift:Double)
                                                                          (implicit d1: DummyImplicit, d2:DummyImplicit):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,null).map(_))
                                    .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))

     val finalRules=dataset.map(new WekaAssociationRulesValidationSparkMapper(headers,null,null).map(_,candidateRules))
                                .reduce(new WekaAssociationRulesSparkReducer().reduce(_,_))
   
    return finalRules
  }
  
   /**Method that sorts a set of rules based on the desired metric and displays the results
    * 
    * @param rules is the HashMap that contains the rules (returned by the Job)
    * */
   def displayRules(rules:HashMap[String,UpdatableRule]):Unit={
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
           //need something to define full, 80% etc support
           case  n if n>2500 => fullsupport(i)=x.getRuleString;i+=1
           case _ =>   lesssupport(o)=x.getRuleString;o+=1
         }}
        println("\n Full Support \n")
        fullsupport.foreach{x => if(x!=null)println(x)} 
        println("\n Less support \n")
        lesssupport.foreach{x => if(x!=null)println(x)}
     
     
   }
}