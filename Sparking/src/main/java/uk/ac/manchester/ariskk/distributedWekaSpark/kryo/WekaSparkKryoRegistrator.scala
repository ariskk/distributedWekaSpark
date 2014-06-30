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

class WekaSparkKryoRegistrator extends KryoRegistrator{
  
  override def registerClasses(kryo: Kryo) {
       kryo.register(classOf[CSVToArffHeaderSparkJob])
        kryo.register(classOf[CSVToArffHeaderSparkMapper])
        kryo.register(classOf[CSVToArffHeaderSparkReducer])
        kryo.register(classOf[CSVToARFFHeaderReduceTask])
        kryo.register(classOf[CSVToARFFHeaderMapTask])
        kryo.register(classOf[ArrayList[Instances]])
        kryo.register(classOf[Instances])
        kryo.register(classOf[Instance])
        kryo.register(classOf[Attribute])
        kryo.register(classOf[AbstractList[Any]])
  }

}