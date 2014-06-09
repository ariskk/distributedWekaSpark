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

class WekaAssociationRulesValidationSparkMapper (headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
    var ruleList:List[AssociationRule]=null

    
    var my_nom2=new ArrayList[String](2)
     my_nom2.add("low")
     my_nom2.add("high")
    val att=new Attribute("att217",my_nom2)
    


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
     
     //println(inst)
     
  def map(rows:Array[String],hashy:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
     
     for (x <-rows){
       inst.add(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x)))
      }
    
    
    asl.setLowerBoundMinSupport(0.1)
    asl.setNumRulesToFind(10)
    asl.buildAssociations(inst)

    
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()

    

    //val hash=new HashMap[String,UpdatableRule]
    for(x<-0 to ruleList.size()-1){
       if(hashy.contains(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence())){
        println("hooray")
        val modifiedRule=hashy(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence())
        modifiedRule.setConsequenceSupport(ruleList.get(x).getConsequenceSupport)
        modifiedRule.setPremiseSupport(ruleList.get(x).getPremiseSupport)
        modifiedRule.setSupportCount(ruleList.get(x).getTotalSupport())
        modifiedRule.setTransactions(ruleList.get(x).getTotalTransactions())
        hashy.update(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence(),modifiedRule)
        
      }
     }

   // println(hash.isEmpty+" "+hash.keys.size)
    return hashy
  }

}