package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRule


class UpdatableRule (rule:AssociationRule) {
  
  var support=rule.getTotalSupport()
  var premise=rule.getPremiseSupport()
  var consequence=rule.getConsequenceSupport()
  var transactions=rule.getTotalTransactions()
  var ruleID=rule.getPremise()+" "+rule.getConsequence()
  
  def getRule():String=return ruleID
  
  def getRuleString:String=return rule.getPremise()+" "+getPremiseSupport+" "+rule.getConsequence()+" "+getConsequenceSupport+" conf:"+getCondidence+
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
  def getRuleSupport:Double=return support/transactions
  
  def getCondidence:Double=return support/premise
  //def setCondidence(conf:Double):Unit=confidence=conf

  def getLift:Double=return support/(premise*consequence)
 // def setSupport(lf:Double):Unit=lift=lf
  
  def getLeverage:Double=return support/transactions-(consequence/transactions)*(premise/transactions)
  //def setLeverage(lev:Int):Unit=leverage=lev
  
  def getConviction:Double=return (1-consequence)/(1-(support/premise))
  //def setConviction(con:Double):Unit=conviction=con
}