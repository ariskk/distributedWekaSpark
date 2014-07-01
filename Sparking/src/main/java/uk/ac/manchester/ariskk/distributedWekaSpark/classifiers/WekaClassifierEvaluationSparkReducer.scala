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
 *    WekaClassifierEvaluationSparkReducer.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierEvaluationReduceTask
import weka.classifiers.evaluation.Evaluation
import java.util.ArrayList
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask._
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.distributed.CSVToARFFHeaderMapTask


/**Classifier evaluation Reduce task
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierEvaluationSparkReducer  extends java.io.Serializable {
  
  //Initialize Base reduce task
  var r_task=new WekaClassifierEvaluationReduceTask
  

  /**Reducer that merges two evaluations
   * 
   * @param evalA evaluations merged so far
   * @param evalB next evaluation to merge
   * @return aggregated evaluations
   * */
  def reduce(evalA:Evaluation,evalB:Evaluation): Evaluation={
    val list=new ArrayList[Evaluation]
    list.add(evalA)
    list.add(evalB)
    return r_task.aggregate(list)
  }
  
}