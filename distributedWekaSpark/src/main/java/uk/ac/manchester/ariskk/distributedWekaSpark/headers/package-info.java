/**
 * SparkJob for building distributed .arff headers for Weka Tasks
 * Mapper computes a partial Header on a dataset partition
 * Reducer merges the Headers to a single Headers that can be distributed to an
 * arbitrary number of tasks or saved to HDFS
 */
/**
 * @author Aris-Kyriakos Koliopoulos (ak.koliopoulos {[at]} gmail {[dot]} com)
 *
 */
package uk.ac.manchester.ariskk.distributedWekaSpark.headers;