package uk.ac.manchester.ariskk.distributedWekaSpark.main


import java.io.BufferedReader
import java.io.FileReader
import weka.core.Instances
import weka.associations.Apriori
import java.util.HashSet
import weka.associations.AssociationRule
import uk.ac.manchester.ariskk.distributedWekaSpark.associationRules.UpdatableRule
import scala.collection.mutable.HashMap
import weka.core.converters.CSVLoader
import weka.core.converters.ConverterUtils.DataSource
import scala.util.matching.Regex
import weka.clusterers.Canopy
import weka.associations.FPGrowth.FrequentItemSets
import weka.associations.FPGrowth
import weka.associations.BinaryItem
import weka.associations.FPGrowth.FrequentBinaryItemSet
import java.util.ArrayList
import weka.core.EuclideanDistance
import weka.filters.SimpleFilter
import weka.core.ChebyshevDistance
import weka.associations.ItemSet
import weka.associations.Item
import java.util.Collection
import weka.associations.DefaultAssociationRule
import weka.core.Instance
import weka.core.DenseInstance
import weka.core.FastVector
import scala.util.control.Breaks._



object testing {

  def main(args: Array[String]): Unit = {

   breakable {
   for(i <-0 to 2){
     
     if(i==1) break
     println("shit")
   }
   }
    
    
    val source = new BufferedReader( new FileReader("/home/weka/Documents/weka-3-7-10/data/supermarket.arff"))
   
    val inst=new Instances(source) 
    
    val asl=new FPGrowth
    asl.setLowerBoundMinSupport(0.1)
    asl.buildAssociations(inst)
    
 
    val rules=asl.getAssociationRules().getRules()
    val ru=asl.getAssociationRules()
    
    val rule=rules.get(0)
    
    val prem=rule.getPremise.asInstanceOf[ArrayList[BinaryItem]]
    println(prem); 
    val con=rule.getConsequence().asInstanceOf[ArrayList[BinaryItem]]
    con.addAll(prem)
    //val iii=new ItemSet(prem)
  
//    val listk=new ArrayList[Object]
//    val listl=new ArrayList[BinaryItem]
//    for(i <- 0 to prem.size-1){listk.add(new BinaryItem(prem.get(i).getAttribute(),prem.get(i).getFrequency()))
//      println(prem.get(i).getAttribute())
//      listl.add(new BinaryItem(prem.get(i).getAttribute(),prem.get(i).getFrequency()))
//    }
//    listl.add(new BinaryItem(rule.getConsequence().asInstanceOf[ArrayList[Item]].get(0).getAttribute(),rule.getConsequence().asInstanceOf[ArrayList[Item]].get(0).getFrequency()))
   // println(rule.getConsequence().asInstanceOf[ArrayList[Item]].get(0).getFrequency())
   val premset=new weka.associations.FPGrowth.FrequentBinaryItemSet(prem,rule.getPremiseSupport())
   val conset=new weka.associations.FPGrowth.FrequentBinaryItemSet(con,rule.getTotalSupport())
   
   //inst.
   println(inst.get(0).isMissing(prem.get(0).getAttribute()))

  // println(inst.get(0).((prem.get(2).getAttribute().index()))
   println(inst.get(0).value(prem.get(3).getAttribute().index()))
   println(prem.get(3).getValueIndex().toDouble)
   if(inst.get(0).value(prem.get(3).getAttribute().index())==prem.get(3).getValueIndex().toDouble){println("sdjlkjlsjld")}
   println("adsda")
   println(inst.get(0).attribute(prem.get(3).getAttribute().index).weight())
   println(prem.get(3).getAttribute().weight())
   exit(0)
   println(inst.get(0).isMissing(inst.get(0).attribute(81)))
   println(prem.get(0).getAttribute())
   val freq=new weka.associations.FPGrowth.FrequentItemSets(inst.size)
  
   
   freq.addItemSet(premset)
   freq.addItemSet(conset)
   println(prem)
   val setList=new ArrayList[Object]
    setList.add(premset)
    setList.add(conset)
    ItemSet.upDateCounters(setList, inst)
    
    val it=new ItemSet(9)
    it.upDateCounter(inst.get(0))
   println(prem)
  // ItemSet.upDateCounters(, inst)
   FPGrowth.generateRulesBruteForce(freq, DefaultAssociationRule.METRIC_TYPE.CONFIDENCE, 0.9, inst.size, 1, inst.size)
    
   exit(0)
    
    var itemlist=rule.getConsequence().asInstanceOf[ArrayList[Item]]
    itemlist.addAll(rule.getPremise())
    val some=itemlist.toArray()
    
    val itemiseti=new ItemSet(10)
    //itemiseti.setItem(x$1)
    val listu=new ArrayList[Object]
    listu.add(itemiseti)
    ItemSet.upDateCounters(listu, inst)
    println(listu.get(0))
    val itemsets=ItemSet.singletons(inst)
    for( i<-0 to itemsets.size-1) { if( itemsets.get(i).asInstanceOf[ItemSet].containedBy(inst.get(0))){println(i)}}
     
   // println(item.containedBy(inst.get(0))) ;exit(0)
    exit(0)
//    val bin=new BinaryItem(null,0)
//    val its=new FrequentBinaryItemSet(null,10)
//    val manyits=new FrequentItemSets(50)
//    
//    its.addItem(bin)
//    
//    manyits.addItemSet(its)
//    FPGrowth.generateRulesBruteForce(manyits, null, 0, 0, 0, 0)
    
   // val stuff=FPGrowth.
    
    
    val source2 = new BufferedReader( new FileReader("/home/weka/Documents/weka-3-7-10/data/iris.arff"))
    val inst2=new Instances(source2)
    val can=new Canopy
    can.buildClusterer(inst2)
   // println(can)
    
     val source3 = new BufferedReader( new FileReader("/home/weka/Documents/weka-3-7-10/data/iris.arff"))
    val inst3=new Instances(source3)
    val can2=new Canopy
    
    can2.buildClusterer(inst3)
    
   // val canopies=can.getCanopies()
   // println(canopies)
    val list=new ArrayList[Canopy]
    list.add(can)
    list.add(can2)
    val dist=new ChebyshevDistance(can.getCanopies())
   
    println(Canopy.aggregateCanopies(list, can.getActualT1(), can.getActualT2(), dist, null, 3))
    
    
    
//  //  test.addItemSet(x$1)
//   
//   // val asl2=new FPGrowth
//   // asl2.setLowerBoundMinSupport(0.1)
//   // asl2.buildAssociations(inst)
//    val ruless=asl.getAssociationRules()
//    val rules=ruless.getRules()
//    //val ruless2=asl2.getAssociationRules()
//   // val rules2=ruless2.getRules()
//    
//     println(asl)
//     val rul=new UpdatableRule(rules.get(0))
//      println(rul.getPremiseString )
//      println(rul.getNumberOfItems)
      
//      println(rules.get(0))
//      println(rules.get(0).getConsequenceSupport()+" "+rules.get(0).getPremiseSupport()+" "+rules.get(0).getTotalSupport()+" "+rules.get(0).getTotalTransactions())
//      println(rul.getConsequenceSupport+" "+rul.getPremiseSupport+" "+rul.getSupportCount+" "+rul.getTransactions)
//      rul.addConsequenceSupport(rules.get(0).getConsequenceSupport())
//      rul.addPremiseSupport(rules.get(0).getPremiseSupport())
//      rul.addTransactions(10)
//      rul.addSupportCount(10)
//      println(rul.getConsequenceSupport+" "+rul.getPremiseSupport+" "+rul.getSupportCount+" "+rul.getTransactions)
//      exit(0)

      
//     
//    val hashmap=new HashMap[String,UpdatableRule]
//    for(x<- 0 to rules.size()-1){hashmap.put(rules.get(x).getPremise()+" "+rules.get(x).getConsequence(),new UpdatableRule(rules.get(x)))}
//    
//    if(hashmap.contains(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())){
//      //support update
//    println(hashmap.get(rules2.get(1).getPremise()+" "+rules2.get(1)))
//    
//    val nrule=hashmap(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())
//    hashmap.remove(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())
//    nrule.setSupportCount(rules2.get(1).getTotalSupport()+nrule.getSupportCount)
//    nrule.addConsequenceSupport(rules2.get(1).getConsequenceSupport())
//    nrule.addPremiseSupport(rules2.get(1).getPremiseSupport())
//    nrule.addTransactions(rules2.get(1).getTotalTransactions())
//    hashmap.put(nrule.getRule,nrule)
//    println(hashmap(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence()).getSupportCount)
//    
//    
//    }
//    val some=hashmap(rules2.get(1).getPremise()+" "+rules2.get(1).getConsequence())
//    println(some.getRuleString)
//    
//    
//    val ll=rules2.get(1).getMetricValuesForRule()
//    
//    val kk=rules2.get(1).getMetricNamesForRule()
   // for(x <-0 to ll.length-1){println(kk(x)+" "+ll(x))}
//    println(rules.get(0).toString)
//   // println(rules.get(0))
//    println(rules.get(0).getConsequence()+"  "+rules.get(0).getConsequenceSupport())
//    println(rules.get(0).getPremise()+"  "+rules.get(0).getPremiseSupport())
//    println(rules.get(0).getTotalTransactions())
//    println(rules.get(0).getTotalSupport())
   
  
  }

}