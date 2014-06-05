package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import java.util.HashSet
import weka.associations.AssociationRule

class WekaAssociationRulesPartitionMiningSparkReducer extends java.io.Serializable{
  
  
  def reduce(rulesA:AssociationRules,rulesB:AssociationRules):AssociationRules={
    
    val rules=rulesA.getRules()
    val rule=rules.get(0)
    val hashset=new HashSet[AssociationRule]
    hashset.add(rule)
    return null
  }

}