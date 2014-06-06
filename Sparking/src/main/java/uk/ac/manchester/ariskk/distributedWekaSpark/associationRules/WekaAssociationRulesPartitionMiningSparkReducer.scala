package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import java.util.HashSet
import weka.associations.AssociationRule
import scala.collection.mutable.HashMap



class WekaAssociationRulesPartitionMiningSparkReducer extends java.io.Serializable{
  
  
  def reduce(rulesMap:HashMap[String,UpdatableRule],newRules:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
    newRules.foreach{
       rule=>{
        if(rulesMap.contains(rule._1)){
        var modifiedRule=rulesMap.remove(rule._1).getOrElse(rule._2)
        modifiedRule.addConsequenceSupport(rule._2.getConsequenceSupport)
        modifiedRule.addPremiseSupport(rule._2.getPremiseSupport)
        modifiedRule.addSupportCount(rule._2.getSupportCount)
        modifiedRule.addTransactions(rule._2.getTransactions)
        rulesMap.put(modifiedRule.getRule,modifiedRule)
      }
      else{
        rulesMap.put(rule._1,rule._2)
      }
    }
    }
    
    return rulesMap
  }

}