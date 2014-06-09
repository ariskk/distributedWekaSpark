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
import weka.distributed.DistributedWekaException
import org.apache.spark.SparkContext

class WekaAssociationRulesPartitionMiningSparkMapper(headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
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
     
   
     
      
//    //Set the classifier to train 
//    val obj=Class.forName(ruleMiner).newInstance()
//    val cla=obj.asInstanceOf[AbstractAssociator] 
//    
//    var asl:AbstractAssociator=null
//    if(cla.isInstanceOf[Apriori]){asl=new Apriori;}
//    else if(cla.isInstanceOf[FPGrowth]){asl=new FPGrowth}
       
     var asl=new Apriori
     var heady=headers
     heady.replaceAttributeAt(att, 216)  ///WHY IS THAT?????
  
    //Remove the summary from the headers
     var strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(heady)
     var inst=new Instances(strippedHeader,0)
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))
     
     //println(inst)
     
  def map(rows:Array[String]):HashMap[String,UpdatableRule]={
     
     for (x <-rows){
       inst.add(m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x)))
      }
   // println(inst.size)


//    println(asl.getOptions().mkString(" "))
//    val source = new BufferedReader( new FileReader("/home/weka/Documents/weka-3-7-10/data/supermarketmod.arff"))
//   
//    val instA=new Instances(source) 
//    println(instA.equalHeadersMsg(inst))
//    println(instA.equalHeaders(inst))
    
    asl.setLowerBoundMinSupport(0.1)
    
    //asl.setNumRulesToFind(10)
    asl.buildAssociations(inst)
    
    
//    if(asl.isInstanceOf[FPGrowth]){
//    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules()}
//    else if(asl.isInstanceOf[Apriori]){
//    ruleList=asl.asInstanceOf[FPGrowth].getAssociationRules().getRules() 
//    }
//    else{throw new DistributedWekaException("Unsupported AssociationRule Miner!")}
    
    println(asl.getAssociationRules().getRules().size)
    ruleList=asl.getAssociationRules().getRules()
   // println(ruleList.get(0))
   // println(ruleList.get(0).getPremise()+" "+ruleList.get(0).getConsequence())
     

    val hash=new HashMap[String,UpdatableRule]
    for(x<-0 to ruleList.size()-1){
      hash+=(ruleList.get(x).getPremise()+" "+ruleList.get(x).getConsequence() -> new UpdatableRule(ruleList.get(x)))
     }

   // println(hash.isEmpty+" "+hash.keys.size)
    return hash
  }

}