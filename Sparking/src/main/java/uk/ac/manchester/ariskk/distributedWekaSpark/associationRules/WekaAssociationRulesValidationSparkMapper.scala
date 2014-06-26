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
import scala.util.control.Breaks._

class WekaAssociationRulesValidationSparkMapper (headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
    var ruleList:List[AssociationRule]=null

     //dummy for supermarket only
     var my_nom2=new ArrayList[String](2)
     my_nom2.add("low")
     my_nom2.add("high")
     val att=new Attribute("total",my_nom2)
    
     var strippedHeader=headers
     strippedHeader.replaceAttributeAt(att, 216)  ///WHY IS THAT????? it does influence ArrayString but not the other two. weird

 
      //Initialize the parser
      var m_rowparser=new CSVToARFFHeaderMapTask()
       //val split=Utils.splitOptions("-N first-last")
      m_rowparser.setOptions(rowparserOptions)
      //Remove the summary from the headers
      strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(strippedHeader)
      m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))

  def map(rows:Array[String],hashmap:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
 
     for (x <-rows){
       var instance=m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x))

      // breakable{
       var bool=true
       hashmap.foreach {
         k =>
           bool=true 

           k._2.addTransactions(1)
           if((!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute()))
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) k._2.addConsequenceSupport(1) //need smarter here
           //make it a while
           breakable{
           for(x <-0 to k._2.getPremiseItems.size()-1){
           if(instance.isMissing(k._2.getPremiseItems.get(x).getAttribute())||
               instance.value(k._2.getPremiseItems.get(x).getAttribute().index)!=k._2.getPremiseItems.get(x).getValueIndex().toDouble) break//{bool=false}//break

           }

          // if(bool){//
           k._2.addPremiseSupport(1)
           if(!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute())
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) {
             k._2.addSupportCount(1)
             }
          // }//breakends
           }
           
       }
      // }
       
       
      }
     

    return hashmap
  }

    
   def map(rows:Array[Instance],hashmap:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
     //val hashy=hashi
     
     for (instance <-rows){
   
       // breakable{
       var bool=true
       hashmap.foreach {
         k =>
           bool=true 

           k._2.addTransactions(1)
           if((!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute()))
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) k._2.addConsequenceSupport(1) //need smarter here
           //make it a while
           breakable{
           for(x <-0 to k._2.getPremiseItems.size()-1){
           if(instance.isMissing(k._2.getPremiseItems.get(x).getAttribute())||
               instance.value(k._2.getPremiseItems.get(x).getAttribute().index)!=k._2.getPremiseItems.get(x).getValueIndex().toDouble) break//{bool=false}//break

           }

          // if(bool){//
           k._2.addPremiseSupport(1)
           if(!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute())
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) {
             k._2.addSupportCount(1)
             }
          // }//breakends
           }
           
       }
      // }
      }

    return hashmap
  }
   
   
  
    def map(instances:Instances,hashmap:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
    // val hashy=hashi

      for(i <-0 to instances.size()-1){
        var instance=instances.get(i)
        
        // breakable{
       var bool=true
      hashmap.foreach {
         k =>
           bool=true 

           k._2.addTransactions(1)
           if((!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute()))
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) k._2.addConsequenceSupport(1) //need smarter here
           //make it a while
           breakable{
           for(x <-0 to k._2.getPremiseItems.size()-1){
           if(instance.isMissing(k._2.getPremiseItems.get(x).getAttribute())||
               instance.value(k._2.getPremiseItems.get(x).getAttribute().index)!=k._2.getPremiseItems.get(x).getValueIndex().toDouble) break//{bool=false}//break

           }

          // if(bool){//
           k._2.addPremiseSupport(1)
           if(!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute())
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) {
             k._2.addSupportCount(1)
             }
          // }//breakends
           }
           
       }
      // }
       }
     return hashmap
  }
    
    def updateHashMap():Unit={}
}