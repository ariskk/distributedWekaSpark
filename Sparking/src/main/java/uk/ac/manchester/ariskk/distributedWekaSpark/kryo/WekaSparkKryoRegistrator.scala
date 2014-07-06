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
 *    WekaSparkKryoRegistrator.scala
 *    Copyright (C) 2014 Koliopoulos Kyriakos-Aris
 *
 */

package uk.ac.manchester.ariskk.distributedWekaSpark.kryo

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkMapper
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkReducer
import java.util.ArrayList
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.distributed.CSVToARFFHeaderMapTask
import weka.core.Instance
import weka.core.Attribute
import java.util.AbstractList
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkJob
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkMapper
import uk.ac.manchester.ariskk.distributedWekaSpark.headers.CSVToArffHeaderSparkReducer
import weka.core.Attribute
import weka.core.Instance
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import weka.distributed.CSVToARFFHeaderReduceTask
import weka.core.DenseInstance
import weka.core.ProtectedProperties

class WekaSparkKryoRegistrator extends KryoRegistrator{
  
  override def registerClasses(kryo: Kryo) {
       kryo.register(classOf[CSVToArffHeaderSparkJob])
        kryo.register(classOf[CSVToArffHeaderSparkMapper])
        kryo.register(classOf[CSVToArffHeaderSparkReducer])
        kryo.register(classOf[CSVToARFFHeaderReduceTask])
        kryo.register(classOf[CSVToARFFHeaderMapTask])
        kryo.register(classOf[ArrayList[Instances]])
        kryo.register(classOf[Instances],new InstancesSerializer)
        kryo.register(classOf[Instance])
        kryo.register(classOf[Attribute],new AttributeSerializer(kryo,classOf[Attribute]))
        kryo.register(classOf[AbstractList[Any]])
        kryo.register(classOf[DenseInstance],new DenseInstanceSerializer)
        kryo.register(classOf[ProtectedProperties],new ProtectedPropertiesSerializer(kryo,classOf[ProtectedProperties]))
     
  }

}