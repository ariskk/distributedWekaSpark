package uk.ac.man.aris

import weka.distributed.CSVToARFFHeaderMapTask
import java.util.ArrayList
import weka.core.Instances
import weka.core.Instance
import scala.collection.mutable.ListBuffer


/**Mapper implementation for CSVToArffHeaderSpark job 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   */

/**Constructor
 * Spark serializes classes before distributing them to the nodes.
 * All classes must be serializable
 * @param CSVToArffMapTask options array of Strings */
class CSVToArffHeaderSparkMapper (options:Array[String] ) extends java.io.Serializable{
 
  var m_task=new CSVToARFFHeaderMapTask
  m_task.setOptions(options)
  
/**   Spark  wrapper for CSVToArffMapTask base task
 *    
 *    @param rows an RDD or HadoopRDD partition
 *    @param names is a list with the attributes names
 *    @return a Header for the processed partition         */
      def map (rows:Array[String],names: ArrayList[String]): Instances ={
        for(i <- rows){
          m_task.processRow(i,names)
        }
        return m_task.getHeader()
      }
 }