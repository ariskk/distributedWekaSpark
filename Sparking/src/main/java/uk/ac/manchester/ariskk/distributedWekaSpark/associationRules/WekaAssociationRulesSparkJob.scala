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
 *    WekaAssociationRulesSparkJob.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

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
  def findAssociationRules (dataset:RDD[String],headers:Instances,parserOptions:Array[String],wekaOptions:Array[String]):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.glom.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,parserOptions,wekaOptions).map(_))
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
  def findAssociationRules (dataset:RDD[Array[Instance]],headers:Instances,parserOptions:Array[String],wekaOptions:Array[String])
                                                                                          (implicit d: DummyImplicit):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,parserOptions,wekaOptions).map(_))
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
  def findAssociationRules (dataset:RDD[Instances],headers:Instances,parserOptions:Array[String],wekaOptions:Array[String])
                                                                          (implicit d1: DummyImplicit, d2:DummyImplicit):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,parserOptions,wekaOptions).map(_))
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
       array.foreach{rule => println(rule.getRuleString)}
     
   }
}