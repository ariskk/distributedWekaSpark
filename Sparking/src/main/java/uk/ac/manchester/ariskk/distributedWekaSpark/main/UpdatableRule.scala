package uk.ac.manchester.ariskk.distributedWekaSpark.main

import weka.associations.AssociationRule


class UpdatableRule (rule:AssociationRule) {
  
  var support=rule.getTotalSupport()
  var premise=rule.getPremiseSupport()
  var consequence=rule.getConsequenceSupport()
  var transactions=rule.getTotalTransactions()
  
  
  def getRuleString:String=return rule.toString()
  
  def getSupportCount:Int=return support
  def setSupportCount(sup:Int):Unit=support=sup
  
  def getPremiseSupport:Int=return premise
  def setPremiseSupport(pr:Int):Unit=premise=pr
  
  def getConsequenceSupport:Int=return consequence
  def setConsequenceSupport(con:Int):Unit=consequence=con
  
  def getTransactions:Int=return transactions
  def setTransactions(tr:Int):Unit=transactions=tr
  
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