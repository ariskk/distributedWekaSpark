package uk.ac.manchester.ariskk.distributedWekaSpark.main


import java.io.BufferedReader
import java.io.FileReader
import weka.core.Instances
import weka.associations.Apriori
import java.util.HashSet
import weka.associations.AssociationRule
import java.util.HashMap
import weka.associations.FPGrowth
import uk.ac.manchester.ariskk.distributedWekaSpark.associationRules.UpdatableRule

object testing {

  def main(args: Array[String]): Unit = {
//    
//    class UpdatableSupportRule (initRule:AssociationRule){
//      val rule:AssociationRule=initRule
//      var support:Int=rule.getTotalSupport()
//      
//      def getSup():Int=return support
//      def setSup(sup:Int):Unit={support=sup}
//      def getRuleString():String=return rule.toString()
//    }
    
    val source = new BufferedReader( new FileReader("/home/weka/Documents/weka-3-7-10/data/supermarket.arff"))
   
    val inst=new Instances(source) 
   
    val asl=new FPGrowth
    asl.setLowerBoundMinSupport(0.1)
    asl.buildAssociations(inst)
    val asl2=new FPGrowth
    asl2.setLowerBoundMinSupport(0.1)
    asl2.buildAssociations(inst)
    val ruless=asl.getAssociationRules()
    val rules=ruless.getRules()
    val ruless2=asl2.getAssociationRules()
    val rules2=ruless2.getRules()
 
    
  
    val hashmap=new HashMap[String,UpdatableRule]
    for(x<- 0 to rules.size()-1){hashmap.put(rules.get(x).getPremise()+" "+rules.get(x).getConsequence(),new UpdatableRule(rules.get(x)))}
    
    if(hashmap.containsKey(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())){
      //support update
    println(hashmap.get(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence()).getSupportCount)
    
    val nrule=hashmap.get(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())
    hashmap.remove(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())
    nrule.setSupportCount(rules2.get(1).getTotalSupport()+nrule.getSupportCount)
    nrule.addConsequenceSupport(rules2.get(1).getConsequenceSupport())
    nrule.addPremiseSupport(rules2.get(1).getPremiseSupport())
    nrule.addTransactions(rules2.get(1).getTotalTransactions())
    hashmap.put(nrule.getRule,nrule)
    println(hashmap.get(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence()).getSupportCount)
    
    
    }
    val some=hashmap.get(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())
    println(some.getRuleString)
    
    
    val ll=rules2.get(1).getMetricValuesForRule()
    
    val kk=rules2.get(1).getMetricNamesForRule()
   // for(x <-0 to ll.length-1){println(kk(x)+" "+ll(x))}
    println(rules.get(0).toString)
   // println(rules.get(0))
    println(rules.get(0).getConsequence()+"  "+rules.get(0).getConsequenceSupport())
    println(rules.get(0).getPremise()+"  "+rules.get(0).getPremiseSupport())
    println(rules.get(0).getTotalTransactions())
    println(rules.get(0).getTotalSupport())
   
  
  }

}