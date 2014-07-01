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
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
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
class WekaAssociationRulesPartitionMiningSparkMapper(headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
    
    var ruleList:List[AssociationRule]=null

    
    var my_nom2=new ArrayList[String](2)
     my_nom2.add("low")
     my_nom2.add("high")
    val att=new Attribute("total",my_nom2)
    


    //Initialize the parser
      var m_rowparser=new CSVToARFFHeaderMapTask()
     // m_rowparser.setOptions(rowparserOptions)
    
    
      val split=Utils.splitOptions("-N first-last")
       m_rowparser.setOptions(split)
     //  println( m_rowparser.getOptions().mkString(" " ))
     
   
     
      
//    //Set the classifier to train 
//    val obj=Class.forName(ruleMiner).newInstance()
//    val cla=obj.asInstanceOf[AbstractAssociator] 
//    
//    var asl:AbstractAssociator=null
//    if(cla.isInstanceOf[Apriori]){asl=new Apriori;}
//    else if(cla.isInstanceOf[FPGrowth]){asl=new FPGrowth}
       
     var asl=new FPGrowth
     var heady=headers
     heady.replaceAttributeAt(att, 216)  ///WHY IS THAT?????
  
    //Remove the summary from the headers
     var strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(heady)
     var inst=new Instances(strippedHeader,0)
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
     
     //println(inst)
     
  def map(rows:Array[String]):HashMap[String,UpdatableRule]={
     
     for (x <-rows){
       inst.add(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x)))
      }


    asl.setMinMetric(0.90)
    asl.setLowerBoundMinSupport(0.1)
 
    asl.setNumRulesToFind(10)
    asl.buildAssociations(inst)
    
    
//    if(asl.isInstanceOf[FPGrowth]){
//    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules()}
//    else if(asl.isInstanceOf[Apriori]){
//    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules() 
//    }
//    else{throw new DistributedWekaException("Unsupported AssociationRule Miner!")}
    println(inst.size)
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()


    val hash=new HashMap[String,UpdatableRule]
    for(x<-0 to ruleList.size()-1){
      //
      val newRule=new UpdatableRule(ruleList.get(x))
      newRule.setConsequenceSupport(0);newRule.setPremiseSupport(0);newRule.setSupportCount(0);newRule.setTransactions(0)
      //println(newRule.getRule);exit(0)
      hash+=(newRule.getRule -> newRule)
     // hash+=(ruleList.get(x).getPremise()+""+ruleList.get(x).getConsequence() -> newRule)
     }
      
    return hash
  }
    
    
    
    
    def map(rows:Array[Instance]):HashMap[String,UpdatableRule]={
     
     for (x <-rows){
       
       inst.add(x)
      }

    asl.setMinMetric(0.90)
    asl.setLowerBoundMinSupport(0.1)
    //hacky trick about attribute
    asl.setNumRulesToFind(10)
    asl.buildAssociations(inst)
    

    println(inst.size)
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()


    val hash=new HashMap[String,UpdatableRule]
    for(x<-0 to ruleList.size()-1){
      //
      val newRule=new UpdatableRule(ruleList.get(x))
      newRule.setConsequenceSupport(0);newRule.setPremiseSupport(0);newRule.setSupportCount(0);newRule.setTransactions(0)
      hash+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() -> newRule)
     }

    return hash
  }
    
    
    def map(instances:Instances):HashMap[String,UpdatableRule]={


    asl.setMinMetric(0.90)
    asl.setLowerBoundMinSupport(0.1)
    //hacky trick about attribute
    asl.setNumRulesToFind(10)
    asl.buildAssociations(instances)

    println(instances.size)
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()


    val hash=new HashMap[String,UpdatableRule]
    for(x<-0 to ruleList.size()-1){
      //
      val newRule=new UpdatableRule(ruleList.get(x))
      newRule.setConsequenceSupport(0);newRule.setPremiseSupport(0);newRule.setSupportCount(0);newRule.setTransactions(0)
      hash+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() -> newRule)
     }

   // println(hash.isEmpty+" "+hash.keys.size)
    return hash
  }

}