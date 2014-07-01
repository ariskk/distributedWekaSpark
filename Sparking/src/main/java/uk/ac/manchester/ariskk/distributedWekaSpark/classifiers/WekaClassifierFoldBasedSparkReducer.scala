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
 *    WekaClassifierFoldBasedSparkReducer.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers
import weka.classifiers.Classifier
import java.util.ArrayList
import weka.distributed.WekaClassifierReduceTask



/**Spark Reducer for training an arbitrary number of folds
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedSparkReducer(folds:Int) extends java.io.Serializable{
  
  //Initialize the Base Reducer Task (classifier aggregator)
  var r_task=new WekaClassifierReduceTask
  
  /** Reducer task of the fold-based classifier training
   *  
   *  Aggregates the classifiers of each fold on all partitions
   *  @param modelsA classifiers aggregated so far
   *  @param modelsB next list of classifiers to aggregate
   *  @return an aggregated list of classifiers
   */
  def reduce(modelsA:ArrayList[Classifier],modelsB:ArrayList[Classifier]): ArrayList[Classifier]={ 
    var toaggregate_models=new ArrayList[ArrayList[Classifier]]
    var aggregated_models=new ArrayList[Classifier]
    for(i<-0 to folds-1){
      toaggregate_models.add(new ArrayList)
      toaggregate_models.get(i).add(modelsA.get(i))  
      toaggregate_models.get(i).add(modelsB.get(i)) 
      aggregated_models.add(r_task.aggregate(toaggregate_models.get(i)))
    }
  
    return aggregated_models
   }
  
  
}