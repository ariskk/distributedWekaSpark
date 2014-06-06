package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import weka.core.Instances
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

class WekaAssociationRulesSparkJob extends java.io.Serializable{
  
  
  def findAssociationRules (headers:Instances,dataset:RDD[String],minSupport:Double,minConfidence:Double,minLift:Double):HashMap[String,UpdatableRule]={
    
     val candidateRules=dataset.glom.map(new WekaAssociationRulesPartitionMiningSparkMapper(headers,null,null).map(_))
                                    .reduce(new WekaAssociationRulesPartitionMiningSparkReducer().reduce(_,_))
    
     val finalRules=dataset.glom.map(new WekaAssociationRulesValidationSparkMapper().map(_))
                                .reduce(new WekaAssociationRulesValidationSparkReducer().reduce(_,_))
    
    return candidateRules
  }

}