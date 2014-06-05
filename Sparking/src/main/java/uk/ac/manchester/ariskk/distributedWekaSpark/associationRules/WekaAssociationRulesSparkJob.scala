package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import weka.core.Instances
import org.apache.spark.rdd.RDD

class WekaAssociationRulesSparkJob extends java.io.Serializable{
  
  
  def findAssociationRules (headers:Instances,dataset:RDD[String],minSupport:Float,minConfidence:Float,minLift:Float):AssociationRules={
    
     val candidateRules=dataset.glom.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers).map(_))
                                    .reduce(new WekaAssociationRulesPartitionMiningSparkReducer().reduce(_,_))
    
     val finalRules=dataset.glom.map(new WekaAssociationRulesValidationSparkMapper().map(_))
                                .reduce(new WekaAssociationRulesValidationSparkReducer().reduce(_,_))
    
    return finalRules
  }

}