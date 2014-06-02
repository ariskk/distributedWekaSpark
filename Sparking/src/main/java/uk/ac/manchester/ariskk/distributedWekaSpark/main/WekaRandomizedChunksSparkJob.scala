package uk.ac.manchester.ariskk.distributedWekaSpark.main

import org.apache.spark.rdd.RDD
import weka.core.Instances
import weka.distributed.CSVToARFFHeaderMapTask
import scala.util.Random
import weka.distributed.CSVToARFFHeaderReduceTask

/**Spark Job that randomizes (shuffles)  a dataset's partitions with numeric class value or stratifies in case of  nominal class value
 * 
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 */
class WekaRandomizedChunksSparkJob {
  
     //Parses CSV rows
     var m_rowParser=new CSVToARFFHeaderMapTask

     
    /**Method that takes a dataset, checks if the class attribute is nominal and either stratifies (if yes) or randomized (if not)
     * 
     * @param dataset is the dataset 
     * @param numOfChunks is the number of randomized/stratified partitions
     * @param headers is the headers file of the dataset
     * @param classIndex is the position of the class attribute
     * @return returns a RDD partitioned and stratified   
     */
    def randomize(dataset:RDD[String],numOfChunks:Int,headers:Instances,classIndex:Int): RDD[String]={
       val strippedHeaders=CSVToARFFHeaderReduceTask.stripSummaryAtts(headers)
       strippedHeaders.setClassIndex(classIndex)
    
       if(!strippedHeaders.classAttribute().isNominal()) {
          m_rowParser.initParserOnly(CSVToARFFHeaderMapTask.instanceHeaderToAttributeNameList(strippedHeaders))
          var randomGen=new Random(1L)
          for(i<-1 to 10){randomGen.nextInt}  //throw away the first 10 numbers. They tend to be less random
          //ToDo: find a way to ensure each chunk has similar class distribution
          //return random shuffling for nominal values as well in this version
          return dataset.repartition(numOfChunks)
       }
       else{
       return dataset.repartition(numOfChunks)
       }
     }

     
    /**Mapper for the stratification tasks*/
    def map(rows:Array[String],strippedHeaders:Instances): Array[String]={
       for(x<-rows){
         m_rowParser.makeInstance(strippedHeaders, true, m_rowParser.parseRowOnly(x))
        }
        return null
    }
   
    
    /**Reducer for the stratification tasks*/
    def reduce(rows:Array[String]) :RDD[String]={
       return null
    }
}