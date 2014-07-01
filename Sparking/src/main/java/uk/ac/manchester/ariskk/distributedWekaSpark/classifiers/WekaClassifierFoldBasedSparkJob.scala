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
 *    WekaClassifierFoldBasedSparkJob.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.classifiers

import weka.core.Instances
import org.apache.spark.rdd.RDD
import weka.classifiers.Classifier
import weka.distributed.WekaClassifierReduceTask
import weka.core.Instance



/**Spark Job for training an arbitrary number of folds on a classifier
 * 
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaClassifierFoldBasedSparkJob extends java.io.Serializable{
  
  
  
  /**Method to build a fold-based model of a classifier. Accepts the data as an RDD[String]
   * 
   * @param dataset is the dataset to process in RDD[String] format
   * @param headers headers file for the dataset
   * @param folds is the number of folds
   * @param classifierToTrain is the classifier
   * @param metaLearner - optional metaLearner
   * @return a trained classifier*/
  def buildFoldBasedModel(dataset:RDD[String],headers:Instances,folds:Int,classifierToTrain:String,metaLearner:String,classIndex:Int): Classifier={
    

     val classifiers=dataset.glom.map(new WekaClassifierFoldBasedSparkMapper(folds,headers,classifierToTrain,metaLearner,classIndex).map(_))
                                .reduce(new WekaClassifierFoldBasedSparkReducer(folds).reduce(_,_))   
                                
   return new WekaClassifierReduceTask().aggregate(classifiers)
  }
  
  /**Method to build a fold-based model of a classifier. Accepts the dataset as an Array[Instace]
   * 
   * @param dataset is the dataset to process in an Array[Instance] format
   * @param headers headers file for the dataset
   * @param folds is the number of folds
   * @param classifierToTrain is the classifier
   * @param metaLearner - optional metaLearner
   * @return a trained classifier*/
  def buildFoldBasedModel(dataset:RDD[Array[Instance]],headers:Instances,folds:Int,classifierToTrain:String,metaLearner:String,classIndex:Int)
                                                                                                                    (implicit d: DummyImplicit) : Classifier={
    

     val classifiers=dataset.map(new WekaClassifierFoldBasedSparkMapper(folds,headers,classifierToTrain,metaLearner,classIndex).map(_))
                                .reduce(new WekaClassifierFoldBasedSparkReducer(folds).reduce(_,_))   
                                
   return new WekaClassifierReduceTask().aggregate(classifiers)
  }
  
  /**Method to build a fold-based model of a classifier. Accepts the dataset as an Instances object
   * 
   * @param dataset is the dataset to process in an Instances object format
   * @param headers headers file for the dataset
   * @param folds is the number of folds
   * @param classifierToTrain is the classifier
   * @param metaLearner - optional metaLearner
   * @return a trained classifier*/
  def buildFoldBasedModel(dataset:RDD[Instances],headers:Instances,folds:Int,classifierToTrain:String,metaLearner:String,classIndex:Int)
                                                                                         (implicit d1: DummyImplicit, d2: DummyImplicit): Classifier={
    

     val classifiers=dataset.map(new WekaClassifierFoldBasedSparkMapper(folds,headers,classifierToTrain,metaLearner,classIndex).map(_))
                                .reduce(new WekaClassifierFoldBasedSparkReducer(folds).reduce(_,_))   
                                
   return new WekaClassifierReduceTask().aggregate(classifiers)
  }

}