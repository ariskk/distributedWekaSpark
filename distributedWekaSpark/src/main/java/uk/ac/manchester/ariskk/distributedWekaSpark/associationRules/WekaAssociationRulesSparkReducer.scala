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
 *    WekaAssociationRulesSparkReducer.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.associationRules

import weka.associations.AssociationRules
import java.util.HashSet
import weka.associations.AssociationRule
import scala.collection.mutable.HashMap


/**Contains a commutative and associative Reduce function that merges two HashMaps and return the result
 * 
 * @author Aris-Kyriakos Koliopoulos ak.koliopoulos {[at]} gmail {[dot]} com
 */
class WekaAssociationRulesSparkReducer extends java.io.Serializable{
  
  /**Takes two mutable HashMaps as parameters, merges them and returns the result
   * 
   * Order is not important
   * @param rulesMapA the first HashMap
   * @param rulesMapB the second HashMap
   * @return the merged HashMap
   * */
  def reduce(rulesMapA:HashMap[String,UpdatableRule],rulesMapB:HashMap[String,UpdatableRule]):HashMap[String,UpdatableRule]={
    var rulesMapA1=rulesMapA
    rulesMapB.foreach{
       rule=>{
        if(rulesMapA1.contains(rule._1)){
        	var modifiedRule=rulesMapA1(rule._1)
        	modifiedRule.addConsequenceSupport(rule._2.getConsequenceSupport)
        	modifiedRule.addPremiseSupport(rule._2.getPremiseSupport)
        	modifiedRule.addSupportCount(rule._2.getSupportCount)
        	modifiedRule.addTransactions(rule._2.getTransactions)
        	rulesMapA1+=(rule._1 ->modifiedRule)
        	modifiedRule=null
      }
      else{
        rulesMapA1+=(rule._1 -> rule._2)
      }
    }
    }

    return rulesMapA1
  }

}