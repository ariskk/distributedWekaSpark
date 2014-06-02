package uk.ac.manchester.ariskk.distributedWekaSpark.main

import weka.core.Utils


class OptionsParser {
  
  /**ToDo: Parse an arbitrary number of options passed from command line using Weka.Utils    */
  def parseOption(opt:Char,option:String):String={
     //extracts an option from a string
     val options="-W weka.classifiers.meta.Bagging"
     val split=Utils.splitOptions(options)
     val optA=Utils.getOption("num-nodes", split)
    
    return null
  } 
}