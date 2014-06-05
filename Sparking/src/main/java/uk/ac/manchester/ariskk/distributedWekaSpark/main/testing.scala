package uk.ac.manchester.ariskk.distributedWekaSpark.main


import java.io.BufferedReader
import java.io.FileReader
import weka.core.Instances
import weka.associations.Apriori
import java.util.HashSet
import weka.associations.AssociationRule
import java.util.HashMap
import weka.associations.FPGrowth

object testing {

  def main(args: Array[String]): Unit = {
    
    class UpdatableSupportRule (initRule:AssociationRule){
      val rule:AssociationRule=initRule
      var support:Int=rule.getTotalSupport()
      
      def getSup():Int=return support
      def setSup(sup:Int):Unit={support=sup}
      def getRuleString():String=return rule.toString()
    }
    
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

    
    
    val hashmap=new HashMap[String,UpdatableSupportRule]
    for(x<- 0 to rules.size()-1){hashmap.put(rules.get(x).toString,new UpdatableSupportRule(rules.get(x)))}
    
    if(hashmap.containsKey(rules2.get(1).toString)){
      //support update
    println(hashmap.get(rules2.get(1).toString).getSup)
    val nrule=hashmap.get(rules2.get(1).toString)
    hashmap.remove(rules2.get(1).toString)
    nrule.setSup(nrule.getSup+rules2.get(1).getTotalSupport())
    hashmap.put(nrule.getRuleString,nrule)
    println(hashmap.get(rules2.get(1).toString).getSup)
    
    
    }
    val ll=rules2.get(1).getMetricValuesForRule()
    val kk=rules2.get(1).getMetricNamesForRule()
    for(x <-0 to ll.length-1){println(kk(x)+" "+ll(x))}
    println(rules.get(0).toString)
    println(rules.get(0))
    
   
  
  }

}