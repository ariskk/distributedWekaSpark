package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import java.util.HashSet
import weka.associations.AssociationRule
import scala.collection.mutable.HashMap



class WekaAssociationRulesSparkReducer extends java.io.Serializable{
  
  
  def reduce(rulesMapA:HashMap[String,UpdatableRule],rulesMapB:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
    println(rulesMapA.isEmpty+" "+rulesMapA.keys.size)
    println(rulesMapB.isEmpty+" "+rulesMapB.keys.size)
    var rulesMapA1=rulesMapA
    rulesMapB.foreach{
       rule=>{
        if(rulesMapA1.contains(rule._1)){
      //  println("hooray")
        var modifiedRule=rulesMapA1(rule._1)
        modifiedRule.addConsequenceSupport(rule._2.getConsequenceSupport)
        modifiedRule.addPremiseSupport(rule._2.getPremiseSupport)
        modifiedRule.addSupportCount(rule._2.getSupportCount)
        modifiedRule.addTransactions(rule._2.getTransactions)
        rulesMapA1+=(rule._1 ->modifiedRule)
        modifiedRule=null
      }
      else{
        rulesMapA1+=(rule._1 -> rule._2)
      }
    }
    }
   // exit(0)
    return rulesMapA1
  }

}