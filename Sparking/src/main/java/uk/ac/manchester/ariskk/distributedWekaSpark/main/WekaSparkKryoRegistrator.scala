package uk.ac.manchester.ariskk.distributedWekaSpark.main

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

class WekaSparkKryoRegistrator extends KryoRegistrator{
  
  override def registerClasses(kryo: Kryo) {
   // kryo.register(classOf[testing])
  }

}