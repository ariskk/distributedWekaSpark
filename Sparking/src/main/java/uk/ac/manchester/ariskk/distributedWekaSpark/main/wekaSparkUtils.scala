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
class wekaSparkUtils {

  
  
  def getNamesFromString(str:String):ArrayList[String]={
    val list=new ArrayList[String]
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