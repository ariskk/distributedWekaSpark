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
 *    CSVToArffHeaderSparkJob.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.headers
import java.util.ArrayList
import weka.core.Instances
import org.apache.spark.rdd.RDD
import weka.distributed.CSVToARFFHeaderMapTask


/**  This Job builds Weka Arff Headers using a provided dataset in RDD[String] 
 *   
 *   @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class CSVToArffHeaderSparkJob {
 
  
 /**Build the Header file
    * 
    * @param numOfAttributes the number of attributes in the dataset
    * @param data is a reference to the RDD of the dataset
    * @return the headers (weka.core.Instances object)
    *   */
  def buildHeaders (options:Array[String],names:ArrayList[String],numOfAttributes:Int,data:RDD[String],genHeaders:Boolean) : Instances = {
     var headers:Instances=null
     //generate headers' names if not provided
     if(names.size()==0){
     for (i <- 1 to numOfAttributes){
       names.add("att"+i)
     }}
     
     
  ///  val tempMapTask=new CSVToARFFHeaderMapTask()
   // val tempOpt=options
   // tempOpt.+("-no-summary-stats")
   // tempMapTask.setOptions(tempOpt)
    
     //tempMapTask.headerAvailableImmediately(names.size, names, new StringBuffer) &&(! tempMapTask.getComputeSummaryStats())
     //Check if headers are available immediately (in case the user specified that there are no nominal attributes (-no-summary-stats option was provided)
    // if(!genHeaders){
    //   headers=tempMapTask.getHeader(names.size, names)
   //  }
    // else{
    // compute headers using map(generate headers for each partition) and reduce (aggregate partial headers)
     headers=data.glom.map(new CSVToArffHeaderSparkMapper(options).map(_,names)).reduce(new CSVToArffHeaderSparkReducer().reduce(_,_))
   // }
     return headers
   }
 }