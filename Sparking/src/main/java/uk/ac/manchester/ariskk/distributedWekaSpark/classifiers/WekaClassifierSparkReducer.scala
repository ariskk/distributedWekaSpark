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
 *    distributedWekaSpark.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.distributed.WekaClassifierReduceTask
import weka.classifiers.Classifier
import java.util.ArrayList


/**Reducer class for the classifier training job
 * 
 * Commutative and associative function that takes two classifiers, aggregates them (merging them if aggregatable or forming a voted ensemlbe if not)
 * and produces one classifier
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com) 
 */
class WekaClassifierSparkReducer (options:Array[String]) extends java.io.Serializable {
     
     //Base distributedWeka task. Contains the Classifier aggregator
     var r_task=new WekaClassifierReduceTask
     
     
  /** Reducer: aggregated classifiers 
   *  
   *  Classifier A and B can exchange roles with no difference in the outcome
   *  @param classifierA Aggregated classifiers so far
   *  @param classifierB The next classifier to aggregate
   *  @return the aggregated classifier*/
  def reduce(classifierA:Classifier,classifierB:Classifier):Classifier={
       var list=new ArrayList[Classifier]
       list.add(classifierA)
       list.add(classifierB)
       
       return r_task.aggregate(list)
     }
 }