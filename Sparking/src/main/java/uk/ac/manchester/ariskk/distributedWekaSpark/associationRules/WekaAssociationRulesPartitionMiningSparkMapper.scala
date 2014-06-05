package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import weka.core.Instances
import weka.associations.Apriori

class WekaAssociationRulesPartitionMiningSparkMapper(headers:Instances) extends java.io.Serializable{
   
  
  def map(rows:Array[String]):AssociationRules={
    //roughly
    var asl=new Apriori
    asl.buildAssociations(headers)
    val test=asl.getAssociationRules()    
    return test
  }

}