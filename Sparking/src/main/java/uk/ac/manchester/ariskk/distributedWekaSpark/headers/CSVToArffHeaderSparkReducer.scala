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
 *    CSVToArffHeaderSparkReducer.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.headers

import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask
import java.util.ArrayList

/**Reducer implementation for CSVToArffHeaderSpark job 
 * 
 * Takes two headers (Instances objects) as input, aggregates them and return a single header
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   */


class CSVToArffHeaderSparkReducer extends java.io.Serializable {

  var r_task=new CSVToARFFHeaderReduceTask
  
  /**   Spark  wrapper for CSVToArffMapTask base task
    *    @param HeaderA represents aggregated headers
    *    @param HeaderB the next Header to aggregate
    *    @return Aggregated headers
    */
  def reduce (headerA:Instances,headerB:Instances): Instances ={
    var list=new ArrayList[Instances]
    list.add(headerA)
    list.add(headerB)     
    return r_task.aggregate(list)
  }
}