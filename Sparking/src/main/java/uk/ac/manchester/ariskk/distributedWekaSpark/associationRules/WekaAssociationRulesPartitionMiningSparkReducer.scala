package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import java.util.HashSet
import weka.associations.AssociationRule
import scala.collection.mutable.HashMap



class WekaAssociationRulesPartitionMiningSparkReducer extends java.io.Serializable{
  
  
  def reduce(rulesMapA:HashMap[String,UpdatableRule],rulesMapB:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
    println(rulesMapA.isEmpty+" "+rulesMapA.keys.size)
    println(rulesMapB.isEmpty+" "+rulesMapB.keys.size)
    rulesMapB.foreach{
       rule=>{
        if(rulesMapA.contains(rule._1)){
         println("hooray")
        var modifiedRule=rulesMapB(rule._1)
        modifiedRule.addConsequenceSupport(rule._2.getConsequenceSupport)
        modifiedRule.addPremiseSupport(rule._2.getPremiseSupport)
        modifiedRule.addSupportCount(rule._2.getSupportCount)
        modifiedRule.addTransactions(rule._2.getTransactions)
        rulesMapA.update(modifiedRule.getRule,modifiedRule)
      }
      else{
        
        rulesMapA+=(rule._1 -> rule._2)
      }
    }
    }
    
    return rulesMapA
  }

}