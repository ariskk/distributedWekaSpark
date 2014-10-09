/*
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 *    WekaAssociationRulesValidationSparkMapper.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

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


/**Mapper implementation for the validation phase of the Association Rules Mining job
 * 
 * It validates each rules in the HashMap against each Instance. Can be slow for large rule sets. 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaAssociationRulesValidationSparkMapper (headers:Instances,ruleMiner:String,rowparserOptions:Array[String]) extends java.io.Serializable{
    var ruleList:List[AssociationRule]=null

    var strippedHeader=headers

     //set options and remove summary stats
     var m_rowparser=new CSVToARFFHeaderMapTask()
     m_rowparser.setOptions(rowparserOptions)
     strippedHeader=CSVToARFFHeaderReduceTask.stripSummaryAtts(strippedHeader)
     m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeader))

  def map(rows:Array[String],hashmap:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
 
     for (x <-rows){
       var instance=m_rowparser.makeInstance(strippedHeader, true, m_rowparser.parseRowOnly(x))

       var bool=true
       hashmap.foreach {
         k =>
           bool=true 

           k._2.addTransactions(1)
           if((!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute()))
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) k._2.addConsequenceSupport(1) 
           breakable{
           for(x <-0 to k._2.getPremiseItems.size()-1){
           if(instance.isMissing(k._2.getPremiseItems.get(x).getAttribute())||
               instance.value(k._2.getPremiseItems.get(x).getAttribute().index)!=k._2.getPremiseItems.get(x).getValueIndex().toDouble) break

           }

           k._2.addPremiseSupport(1)
           if(!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute())
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) {
             k._2.addSupportCount(1)
             }
           }          
       }
        
      }

    return hashmap
  }

    
   def map(rows:Array[Instance],hashmap:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
     
     for (instance <-rows){
       hashmap.foreach {
         k =>
           k._2.addTransactions(1)
           if((!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute()))
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) k._2.addConsequenceSupport(1) //need smarter here
           breakable{
           for(x <-0 to k._2.getPremiseItems.size()-1){
           if(instance.isMissing(k._2.getPremiseItems.get(x).getAttribute())||
               instance.value(k._2.getPremiseItems.get(x).getAttribute().index)!=k._2.getPremiseItems.get(x).getValueIndex().toDouble) break
           }
           k._2.addPremiseSupport(1)
           if(!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute())
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) {
             k._2.addSupportCount(1)
             }
           }    
       }
      }

    return hashmap
  }
   
   
  
    def map(instances:Instances,hashmap:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={

      for(i <-0 to instances.size()-1){
        var instance=instances.get(i)

      hashmap.foreach {
         k =>
           k._2.addTransactions(1)
           if((!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute()))
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) k._2.addConsequenceSupport(1)
           breakable{
           for(x <-0 to k._2.getPremiseItems.size()-1){
           if(instance.isMissing(k._2.getPremiseItems.get(x).getAttribute())||
               instance.value(k._2.getPremiseItems.get(x).getAttribute().index)!=k._2.getPremiseItems.get(x).getValueIndex().toDouble) break

           }
           k._2.addPremiseSupport(1)
           if(!instance.isMissing(k._2.getConsequenceItems.get(0).getAttribute())
               &&instance.value(k._2.getConsequenceItems.get(0).getAttribute().index)==k._2.getConsequenceItems.get(0).getValueIndex().toDouble) {
             k._2.addSupportCount(1)
             }
           }     
       }
       }
     return hashmap
  }
    
}