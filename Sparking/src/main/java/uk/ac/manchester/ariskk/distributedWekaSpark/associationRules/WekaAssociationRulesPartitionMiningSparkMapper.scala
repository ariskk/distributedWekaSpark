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
 *    WekaAssociationRulesPartitionMiningSparkMapper.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import weka.core.Instances
import weka.associations.Apriori
import weka.associations.AssociationRulesProducer
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.associations.AbstractAssociator
import weka.associations.FPGrowth
import java.util.ArrayList
import weka.associations.AssociationRule
import java.util.List
import scala.collection.mutable.HashMap
import weka.core.Instance
import weka.core.Attribute
import weka.core.converters.CSVSaver
import weka.core.converters.CSVLoader
import weka.core.Utils
import java.io.BufferedReader
import java.io.FileReader
import weka.distributed.DistributedWekaException
import org.apache.spark.SparkContext

/**Mapper implementation for the partition mining phase of the Association Rules Mining job
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaAssociationRulesPartitionMiningSparkMapper(headers:Instances,ruleMiner:String,rowparserOptions:Array[String],ruleMinerOptions:Array[String]) extends java.io.Serializable{
    var ruleList:List[AssociationRule]=null

    var m_rowparser=new CSVToARFFHeaderMapTask()

    m_rowparser.setOptions(rowparserOptions)
     
   
     
      
//   
//    val obj=Class.forName(ruleMiner).newInstance()
//    val cla=obj.asInstanceOf[AbstractAssociator] 
//    
//    var asl:AbstractAssociator=null
//    if(cla.isInstanceOf[Apriori]){asl=new Apriori;}
//    else if(cla.isInstanceOf[FPGrowth]){asl=new FPGrowth}
       
    //Apriori can be used here. Need to provide a user option on this. However asls do not have a common abstract interface as classifiers
     var associationRulesLearner=new FPGrowth
     associationRulesLearner.setOptions(ruleMinerOptions)
     var datasetHeaders=headers
     

     var strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(datasetHeaders)
     var inst=new Instances(strippedHeader,0)
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
     
     
  def map(rows:Array[String]):HashMap[String,UpdatableRule]={
     
     for (x <-rows){
       inst.add(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x)))
      }

    associationRulesLearner.buildAssociations(inst)
    ruleList=associationRulesLearner.getAssociationRules().getRules()
    val hash=new HashMap[String,UpdatableRule]
    
    for(x<-0 to ruleList.size()-1){
      val newRule=new UpdatableRule(ruleList.get(x))
      newRule.setConsequenceSupport(0);newRule.setPremiseSupport(0);newRule.setSupportCount(0);newRule.setTransactions(0)

      hash+=(newRule.getRule -> newRule)
     }
      
    return hash
  }
    
    
    
    
    def map(rows:Array[Instance]):HashMap[String,UpdatableRule]={
     
     for (x <-rows){
      inst.add(x)
     }
     
     associationRulesLearner.buildAssociations(inst)
     ruleList=associationRulesLearner.getAssociationRules().getRules()
     val hash=new HashMap[String,UpdatableRule]
     
     for(x<-0 to ruleList.size()-1){
       val newRule=new UpdatableRule(ruleList.get(x))
       newRule.setConsequenceSupport(0);newRule.setPremiseSupport(0);newRule.setSupportCount(0);newRule.setTransactions(0)
       hash+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() -> newRule)
     }
    return hash
  }
    
    
    def map(instances:Instances):HashMap[String,UpdatableRule]={

		associationRulesLearner.buildAssociations(instances)
		ruleList=associationRulesLearner.getAssociationRules().getRules()
        val hash=new HashMap[String,UpdatableRule]
		
		for(x<-0 to ruleList.size()-1){
		  val newRule=new UpdatableRule(ruleList.get(x))
		  newRule.setConsequenceSupport(0);newRule.setPremiseSupport(0);newRule.setSupportCount(0);newRule.setTransactions(0)
		  hash+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() -> newRule)
		}
     return hash
	}

}