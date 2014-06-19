package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import scala.collection.mutable.HashMap
import weka.associations.AssociationRule
import java.util.ArrayList
import weka.core.Attribute
import weka.distributed.CSVToARFFHeaderMapTask
import weka.core.Utils
import weka.associations.FPGrowth
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask
import java.util.List
import weka.associations.DefaultAssociationRule
import weka.associations.Apriori
import scala.util.Random
import weka.core.Instance

class WekaAssociationRulesValidationSparkMapper (headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
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
       println( m_rowparser.getOptions().mkString(" " ))
     

       
     var asl=new FPGrowth
     var heady=headers
     heady.replaceAttributeAt(att, 216)  ///WHY IS THAT?????
  
    //Remove the summary from the headers
     var strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(heady)
     var inst=new Instances(strippedHeader,0)
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
     
   //  println(strippedHeader)
     
  def map(rows:Array[String],hashi:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
     val hashy=hashi
     
     for (x <-rows){
       inst.add(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x)))
      }
    //asl.setNumRulesToFind(hashi.keys.size) 
    asl.setMinMetric(0.9)
    asl.setLowerBoundMinSupport(0.1)
   // asl.setDelta(0.1)
 
    asl.setFindAllRulesForSupportLevel(true)
    asl.buildAssociations(inst)
    
    
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()
    
    val hashyB=new HashMap[String,UpdatableRule]
    var updatedRule:UpdatableRule=null
    
   // println(ruleList.size);exit(0)
    //val hash=new HashMap[String,UpdatableRule]
//    for(i<- 0 to ruleList.size()-1){
//      
//      if((ruleList.get(i).getPremise().toString.contains("fruit=t, vegetables=t, biscuits=t, total=high"))){ println(ruleList.get(i)+"    "+ruleList.get(i).getTotalTransactions())}}
//    val rand=new Random
//    println(rand.nextInt)
  
    for(x<-0 to ruleList.size()-1){
      
       if(hashy.contains(ruleList.get(x).getPremise().toString+" "+ruleList.get(x).getConsequence().toString)){
         
         println("hooray")
        
        updatedRule=hashy(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence())
        updatedRule.setConsequenceSupport(ruleList.get(x).getConsequenceSupport)
        updatedRule.setPremiseSupport(ruleList.get(x).getPremiseSupport)
        updatedRule.setSupportCount(ruleList.get(x).getTotalSupport())
        updatedRule.setTransactions(ruleList.get(x).getTotalTransactions())
        hashy+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence()->updatedRule)
        hashyB+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() ->updatedRule)
        updatedRule=null
      }
     }

        
     // println(hashy.isEmpty+" "+hashy.keys.size)
    return hashy
  }

    
   def map(rows:Array[Instance],hashi:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
     val hashy=hashi
     
     for (x <-rows){
       inst.add(x)
      }
    //asl.setNumRulesToFind(hashi.keys.size) 
    asl.setMinMetric(0.9)
    asl.setLowerBoundMinSupport(0.08)
   // asl.setDelta(0.1)
 
    asl.setFindAllRulesForSupportLevel(true)
    asl.buildAssociations(inst)
    
    
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()
    
    val hashyB=new HashMap[String,UpdatableRule]
    var updatedRule:UpdatableRule=null

  
    for(x<-0 to ruleList.size()-1){
      
       if(hashy.contains(ruleList.get(x).getPremise().toString+" "+ruleList.get(x).getConsequence().toString)){
         
         println("hooray")
        
        updatedRule=hashy(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence())
        updatedRule.setConsequenceSupport(ruleList.get(x).getConsequenceSupport)
        updatedRule.setPremiseSupport(ruleList.get(x).getPremiseSupport)
        updatedRule.setSupportCount(ruleList.get(x).getTotalSupport())
        updatedRule.setTransactions(ruleList.get(x).getTotalTransactions())
        hashy+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence()->updatedRule)
        hashyB+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() ->updatedRule)
        updatedRule=null
      }
     }

    return hashy
  }
   
   
  
    def map(instances:Instances,hashi:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
     val hashy=hashi

    //asl.setNumRulesToFind(hashi.keys.size) 
    asl.setMinMetric(0.9)
    asl.setLowerBoundMinSupport(0.1)
   // asl.setDelta(0.1)
 
    asl.setFindAllRulesForSupportLevel(true)
    asl.buildAssociations(instances)
    
    
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()
    
    val hashyB=new HashMap[String,UpdatableRule]
    var updatedRule:UpdatableRule=null

  
    for(x<-0 to ruleList.size()-1){
      
       if(hashy.contains(ruleList.get(x).getPremise().toString+" "+ruleList.get(x).getConsequence().toString)){
         
         println("hooray")
        
        updatedRule=hashy(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence())
        updatedRule.setConsequenceSupport(ruleList.get(x).getConsequenceSupport)
        updatedRule.setPremiseSupport(ruleList.get(x).getPremiseSupport)
        updatedRule.setSupportCount(ruleList.get(x).getTotalSupport())
        updatedRule.setTransactions(ruleList.get(x).getTotalTransactions())
        hashy+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence()->updatedRule)
        hashyB+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() ->updatedRule)
        updatedRule=null
      }
     }

        
     // println(hashy.isEmpty+" "+hashy.keys.size)
    return hashy
  }
    
    def updateHashMap():Unit={}
}