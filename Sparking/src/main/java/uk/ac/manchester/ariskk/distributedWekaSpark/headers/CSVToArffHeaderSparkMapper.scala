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
 *    CSVToArffHeaderSparkMapper.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.headers

import weka.distributed.CSVToARFFHeaderMapTask
import java.util.ArrayList
import weka.core.Instances


/**Mapper implementation for CSVToArffHeaderSpark job 
 * 
 * It parses a dataset parition and produces a header
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *   */

/**Constructor
 * Spark serializes classes before distributing them to the nodes.
 * All classes must be serializable
 * @param CSVToArffMapTask options in an array of Strings 
 */
class CSVToArffHeaderSparkMapper (options:Array[String]) extends java.io.Serializable{
 
  //Initialize Base Header Map Task (processes a set of rows in CSV format and produces a header
  var m_task=new CSVToARFFHeaderMapTask
  m_task.setOptions(options)
  
/**   Spark  wrapper for CSVToArffMapTask base task
 *    
 *    @param rows an RDD or HadoopRDD partition in Array[String] format
 *    @param names is a list with the attributes names
 *    @return a Header for the processed partition     
 */
      def map (rows:Array[String],names: ArrayList[String]): Instances ={
        for(x <- rows){
          m_task.processRow(x,names)
        }
        return m_task.getHeader()
      }
 }