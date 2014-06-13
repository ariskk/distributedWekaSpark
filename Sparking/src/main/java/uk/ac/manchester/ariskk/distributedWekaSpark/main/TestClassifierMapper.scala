package uk.ac.manchester.ariskk.distributedWekaSpark.main

import weka.classifiers.Classifier
import weka.core.Instances
import weka.classifiers.trees.J48
import weka.classifiers.bayes.NaiveBayes

class TestClassifierMapper extends java.io.Serializable {

   val classi=new NaiveBayes
  def map(ds:Instances):Classifier={
    classi.buildClassifier(ds)
    return classi
  }
}