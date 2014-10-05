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
 *    WekaSparkUtils.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.main

import java.util.ArrayList
import weka.core.Instances
import java.io.BufferedReader
import weka.classifiers.Classifier
import weka.clusterers.Clusterer

/**Contains various utilities
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class wekaSparkUtils extends java.io.Serializable{

  
  
  def getNamesFromString(str:String):ArrayList[String]={
    val list=new ArrayList[String]
    if(str==null)return list
    val nm=str.split(",")
    for(i<-0 to nm.length-1){list.add(nm(i))}
    return list
  }
   
  
  def convertDeserializedObjectToInstances(obj:Object):Instances={
    return obj.asInstanceOf[Instances]
  }
  def convertDeserializedObjectToClassifier(obj:Object):Classifier={
    return obj.asInstanceOf[Classifier]
  }
  def convertDeserializedObjectToCLusterer(obj:Object):Clusterer={
    return obj.asInstanceOf[Clusterer]
   }

}