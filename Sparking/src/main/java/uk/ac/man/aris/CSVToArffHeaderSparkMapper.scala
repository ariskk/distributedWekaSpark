package uk.ac.man.aris

import weka.distributed.CSVToARFFHeaderMapTask
import java.util.ArrayList
import weka.core.Instances

class CSVToArffHeaderSparkMapper (options:Array[String] ) extends java.io.Serializable{
  
  var m_task=new CSVToARFFHeaderMapTask
  m_task.setOptions(options)
  
  
  
  
  
    
   def map (row:String,m_attNames:ArrayList[String]): Instances={
     m_task.processRow(row, m_attNames)
     return m_task.getHeader()
   }
    
  
}