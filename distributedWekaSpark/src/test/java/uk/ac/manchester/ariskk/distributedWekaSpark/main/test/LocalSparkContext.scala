package uk.ac.manchester.ariskk.distributedWekaSpark.main.test

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

trait LocalSparkContext extends BeforeAndAfterAll{ self: Suite => @transient var sc:SparkContext = _
  
  override def beforeAll(){
    val conf=new SparkConf().setMaster("local").setAppName("test")
    sc=new SparkContext(conf)
    super.beforeAll()  
}  
  
  override def afterAll(){
    if(sc!=null){
      sc.stop()
      
    }
    super.afterAll()
  }

}