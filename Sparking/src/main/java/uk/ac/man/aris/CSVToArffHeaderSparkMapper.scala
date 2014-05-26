package uk.ac.man.aris

import weka.distributed.CSVToARFFHeaderMapTask
import java.util.ArrayList
import weka.core.Instances
import weka.core.Instance
import scala.collection.mutable.ListBuffer



class CSVToArffHeaderSparkMapper (options:Array[String] ) extends java.io.Serializable{
 // 
  var m_task=new CSVToARFFHeaderMapTask
  m_task.setOptions(options)
  
  
  
  ///this line-line mapping and the m_task does chunk-line mapping thus producing n^2 instances
    
   def map (row:String,m_attNames:ArrayList[String]): Instances={
    m_task.processRow(row, m_attNames)
    
    return m_task.getHeader()
   }
  
  
    def map1 (rows:Iterator[String],m_attNames:ArrayList[String]): Instances={
   
      m_task.processRow(rows.next, m_attNames)
    
    return m_task.getHeader()
   }
    
    
      def map2 (rows:Iterator[String],m_attNames:ArrayList[String]): Iterator[Instances]={
   
       m_task.processRow(rows.next, m_attNames)
       //println(m_task.getHeader.toString())
       return Iterator(m_task.getHeader())
   }
      
      
       def map4 (rows:String,m_attNames:ArrayList[String]): TraversableOnce[Instances]={
   
      m_task.processRow(rows, m_attNames)
    
       return ListBuffer(m_task.getHeader())
   }
      def mapf (rows:Array[String],names: ArrayList[String]): Instances ={
        for(i <- rows){
          m_task.processRow(i,names)
        }
        
        return m_task.getHeader()
      }
    
}