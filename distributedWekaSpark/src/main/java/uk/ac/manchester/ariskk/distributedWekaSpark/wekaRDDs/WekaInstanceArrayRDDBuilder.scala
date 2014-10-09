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
 *    WekaInstanceArrayRDDBuilder.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.wekaRDDs

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.Instance
import weka.core.Utils

/**Class that contains a map that parses an Array[String] and produces an Array[Instance]
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaInstanceArrayRDDBuilder(headers:Instances) extends java.io.Serializable {
  
  var m_rowparser=new CSVToARFFHeaderMapTask()
  val stripped= CSVToARFFHeaderReduceTask.stripSummaryAtts(headers) 
  m_rowparser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(stripped))
   
   /**
   * Map function that converts each csv row to an Instance object and adds it an Array
   * 
   * @param rows is dataset partition
   * @return an Array of Instance objects
   */
  def map(rows:Array[String]):Array[Instance]={
       
     var instanceArray=new Array[Instance](rows.length)
     var j=0  
     for (x <- rows){
         instanceArray(j)=m_rowparser.makeInstance(stripped, true, m_rowparser.parseRowOnly(x))
         j+=1
      }
       return instanceArray
     }

}
  
  
