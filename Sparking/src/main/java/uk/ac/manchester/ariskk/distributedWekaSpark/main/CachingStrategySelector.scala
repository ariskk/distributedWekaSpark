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
 *    CachingStrategySelector.scala
 *    Copyright (C) 2014 School of Computer Science, University of Manchester
 *
 */


package uk.ac.manchester.ariskk.distributedWekaSpark.main

import org.apache.spark.storage.StorageLevel

/** 
 *  Class that automatically selects a Storage Level and decides on whether kryo and/or compression should be used
 *  
 *  @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *  */
class CachingStrategySelector (size:Long,totalMem:Double,overhead:Double){
  
  var kryo=false
  var compress=false
  var storageLevel=StorageLevel.DISK_ONLY
  var totalM=totalMem*1024*1024*1024
  
  if(totalM>size*overhead){
    storageLevel=StorageLevel.MEMORY_AND_DISK
  }
  else{
    if(totalM>size){
      storageLevel=StorageLevel.MEMORY_AND_DISK_SER
      kryo=true
    }
    else{
      if(totalM>0.5*size){
        storageLevel=StorageLevel.MEMORY_AND_DISK_SER
        kryo=true
        compress=true
        }
       else{
        storageLevel=StorageLevel.DISK_ONLY
       }
      }
    }
    

  
  
  
  
  def getStorageLevel():StorageLevel={
    return storageLevel
  }
  
  def getUseKryo():Boolean={
    return kryo
  }
  
  def getUseCompression():Boolean={
    return compress
  }

}