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
import weka.core.Instance
import weka.core.Attribute
import weka.core.converters.CSVSaver
import weka.core.converters.CSVLoader
import weka.core.Utils
import java.io.BufferedReader
import java.io.FileReader

class WekaAssociationRulesPartitionMiningSparkMapper(headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
    var ruleList:List[AssociationRule]=null
    
    
    var my_nominal_values = new ArrayList[String](1); 
     my_nominal_values.add("t"); 
    
    var my_nom2=new ArrayList[String](2)
     my_nom2.add("high")
     my_nom2.add("low")
     
    val listw=new ArrayList[Attribute]
    for(i<-1 to 216){listw.add(new Attribute("att"+i,my_nominal_values))}
    val att=new Attribute("att218",my_nom2)
    
   // att.
     listw.add(new Attribute("att217",my_nom2))
      var inst=new Instances("some",listw,217)
      val head=inst
    //Initialize the parser
      var m_rowparser=new CSVToARFFHeaderMapTask()
     // m_rowparser.setOptions(rowparserOptions)
      val split=Utils.splitOptions("-N first-last")
       m_rowparser.setOptions(split)
       println( m_rowparser.getOptions().mkString(" " ))
       exit(0)
      /// println(m_rowparser.getOptions().mkString(" "))
      // exit(0)
      //println(inst)
     
      
//    //Set the classifier to train 
//    val obj=Class.forName(ruleMiner).newInstance()
//    val cla=obj.asInstanceOf[AbstractAssociator] 
//    
//    var asl:AbstractAssociator=null
//    if(cla.isInstanceOf[Apriori]){asl=new Apriori;}
//    else if(cla.isInstanceOf[FPGrowth]){asl=new FPGrowth}
       
    var asl=new FPGrowth
   // asl.setLowerBoundMinSupport(0.10)
    //Remove the summary from the headers. Set the class attribute
     var strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
     inst=strippedHeader
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
     //strippedHeader.setClassIndex(216)
     println(inst)
  def map(rows:Array[String]):HashMap[String,UpdatableRule]={
     
     for (x <-rows){
       inst.add(m_rowparser.makeInstance(strippedHeader, false, m_rowparser.parseRowOnly(x)))
     }
    println(inst.size)
    
    
    asl.setMaxNumberOfItems(5)
    asl.setPositiveIndex(2)
    asl.setNumRulesToFind(10)
    asl.setDelta(0.05)
    asl.buildAssociations(inst)
    
    
//    if(asl.isInstanceOf[FPGrowth]){
//    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules()}
//    else if(asl.isInstanceOf[Apriori]){
//    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules() 
//    }
//    else{throw new Exception("Unsupported AssociationRule Miner!")}
    
    
    ///This is  good
    ruleList=asl.getAssociationRules().getRules()
    println(ruleList.get(0))
    println(ruleList.get(0).getPremise()+" "+ruleList.get(0).getConsequence())


    val hash=new HashMap[String,UpdatableRule]
    for(x<-0 to ruleList.size()-1){
      hash+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() -> new UpdatableRule(ruleList.get(x)))
     }

    println(hash.isEmpty+" "+hash.keys.size)
    return hash
  }

}