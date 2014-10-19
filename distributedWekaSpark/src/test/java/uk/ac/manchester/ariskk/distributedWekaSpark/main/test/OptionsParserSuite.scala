package uk.ac.manchester.ariskk.distributedWekaSpark.main.test

import org.scalatest.FunSuite
import uk.ac.manchester.ariskk.distributedWekaSpark.main.OptionsParser
import org.apache.spark.storage.StorageLevel

object OptionsParserSuite {

  
class OptionsParserSuite extends FunSuite with LocalSparkContext {
  
  test("OptionsParser"){
    val options="""-task buildClassifier -dataset-Type Instances -caching MEMORY_ONLY -master local -hdfs-dataset-path hdfs://test/test -hdfs-names-path hdfs://test/names
                   -hdfs-headers-path hdfs://test/heders -hdfs-classifer-path hdfs://test/classifier -hdfs-output-path hdfs://test/output -num-of-partitions 14 -num-random-chunks 8 
                   -num-of-attributes 17 -class-index 16 -names name1,name2,name3 -num-folds 10 -gen-headers y -classifier weka.classifiers.bayes.NaiveBayes -meta weka.classifiers.meta.Vote
                   -rule-learner weka.associations.FPGrowth -clusterer weka.clusterers.Canopy -num-clusters 7 -distance-metric Euclidean -weka-options \"-depth 3 \"
                   -parser-options \"-N first\" -compress y -kryo y -cache-fraction 0.7 -overhead 4 -locality-wait 3 -cluster-memory 300 """
    val opts=new OptionsParser(options)
    
    
    assert(opts.getTask=="buildClassifier")
    assert(opts.getDatasetType=="Instances")
    assert(opts.getCachingStrategy==StorageLevel.MEMORY_ONLY)
    assert(opts.getMaster=="local")
    assert(opts.getHdfsDatasetInputPath=="hdfs://test/test")
    assert(opts.getNamesPath=="hdfs://test/names")
    assert(opts.getHdfsHeadersInputPath=="hdfs://test/headers")
    assert(opts.getHdfsClassifierInputPath=="hdfs://test/classifier")
    assert(opts.getHdfsOutputPath=="hdfs://test/output")
    assert(opts.getNumberOfPartitions==14)
    assert(opts.getNumberOfRandomChunks==8)
    assert(opts.getNumberOfAttributes==17)
    assert(opts.getClassIndex==16)
    assert(opts.getNames=="name1,name2,name3")
    assert(opts.getNumberOfFolds==10)
    assert(opts.generateHeaderStatistics)
    assert(opts.getClassifier=="weka.classifiers.NaiveBayes")
    
  }
  
  
}
}