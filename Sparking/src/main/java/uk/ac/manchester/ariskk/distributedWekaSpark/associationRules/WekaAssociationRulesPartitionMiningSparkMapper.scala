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

class WekaAssociationRulesPartitionMiningSparkMapper(headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
    var ruleList:List[AssociationRule]=null
    //Initialize the parser
    var m_rowparser=new CSVToARFFHeaderMapTask()
    m_rowparser.setOptions(rowparserOptions)
   
    //Set the classifier to train 
    val obj=Class.forName(ruleMiner).newInstance()
    val cla=obj.asInstanceOf[AssociationRulesProducer] 
    
    var asl:AbstractAssociator=null
    if(cla.isInstanceOf[Apriori]){asl=new Apriori}
    else if(cla.isInstanceOf[FPGrowth]){asl=new FPGrowth}
  
    //Remove the summary from the headers. Set the class attribute
    var strippedHeader:Instances=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
    m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
    
  def map(rows:Array[String]):HashMap[String,UpdatableRule]={
     for (x <-rows){
       strippedHeader.add(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x)))
     }
     
   
    asl.buildAssociations(headers)
    
    if(asl.isInstanceOf[FPGrowth]){
    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules()}
    else if(asl.isInstanceOf[FPGrowth]){
    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules() 
    }
    else{throw new Exception("Unsupported AssociationRule Miner!")}
    val hash=new HashMap[String,UpdatableRule]
    for(x<-0 to ruleList.size()){
      hash.put(ruleList.get(x).toString(), new UpdatableRule(ruleList.get(x)))
    }
    return hash
  }

}