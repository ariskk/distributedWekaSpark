package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRule
import weka.associations.Item

/**Wrapper class for Weka's AssociationRule class that allows to update support,premise,consequence and transactions counts using aggregated partition values 
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com) 
 */
class UpdatableRule (rule:AssociationRule) extends java.io.Serializable with Ordered[UpdatableRule]{
  //To-Do : accept accuracies
  var support=rule.getTotalSupport()
  var premise=rule.getPremiseSupport()
  var consequence=rule.getConsequenceSupport()
  var transactions=rule.getTotalTransactions()
  val ruleID=rule.getPremise()+" "+rule.getConsequence()
  val premiseItems=rule.getPremise()
  val consequenceItems=rule.getConsequence()
  //
  val premiseString=makeString(rule.getPremise().toArray)
  val consequeceString=makeString(rule.getConsequence().toArray)
  val numOfItems=rule.getPremise().toArray.size+rule.getConsequence().toArray.size
  
  def getPremiseString:String=return premiseString
  def getConsequenceString:String=return consequeceString
  //
  
  def getRule:String=return ruleID
  def getNumberOfItems:Int=return numOfItems
  def getRuleString:String=return rule.getPremise()+" "+getPremiseSupport+" "+rule.getConsequence()+" "+getSupportCount+" conf:"+getCondidence+
                                  " lift:"+getLift+" leverage:"+getLeverage+" conviction:"+getConviction
  
  def getSupportCount:Int=return support
  def setSupportCount(sup:Int):Unit=support=sup
  def addSupportCount(sup:Int):Unit=support+=sup
  
  
  
  def getPremiseSupport:Int=return premise
  def setPremiseSupport(pr:Int):Unit=premise=pr
  def addPremiseSupport(pr:Int):Unit=premise+=pr
  
  def getConsequenceSupport:Int=return consequence
  def setConsequenceSupport(con:Int):Unit=consequence=con
  def addConsequenceSupport(con:Int):Unit=consequence+=con
  
  def getTransactions:Int=return transactions
  def setTransactions(tr:Int):Unit=transactions=tr
  def addTransactions(tr:Int):Unit=transactions+=tr
  
  //semantic checking
  def getRuleSupport:Double=if(transactions>0)return round(support.toDouble/transactions.toDouble) else return 0
  
  
  def getCondidence:Double=if(premise>0)return round(support.toDouble/premise.toDouble) else return 0


  def getLift:Double=if(premise*consequence>0)return round((support.toDouble*transactions.toDouble)/(premise.toDouble*consequence.toDouble)) else return 0

  
  def getLeverage:Double=if(transactions>0)return round(support.toDouble/transactions.toDouble-(consequence.toDouble/transactions.toDouble)*(premise.toDouble/transactions.toDouble)) else return 0

  
  def getConviction:Double={
    if(1-(support.toDouble/premise.toDouble)==0){return 0}
    return round((1-(consequence.toDouble/transactions.toDouble))/(1-(support.toDouble/premise.toDouble)))}

  
  def round(num:Double):Double=return BigDecimal(num).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
  
  def compare(that:UpdatableRule):Int=(that.getCondidence) compare (this.getCondidence)
  
  
  //
  def makeString(arrObj:Array[Object]):String={
    var str=""
    arrObj.foreach{x => str+=(x.toString.trim.replace("'","").split("=")(0)+",")}
    return str
  }
}