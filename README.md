distributedWekaSpark  
========
<br>
<img src="https://travis-ci.org/ariskk/distributedWekaSpark.svg?branch=master" alt="Build Status" style="max-width:100%;">

UPDATE
This repository has been inactive for more than 4 years and requires a full rewrite in order to be any useful.
I doubt I will ever get the time to do it.

This repository contains Map and Reduce Wrappers of <b>Weka</b> classes for the Apache <b>Spark</b> distributed 
computing framework, as well as a set of utilities and a commmand line interface that enable the execution of Data Mining jobs
on a Spark cluster.

This project was developed in the School of Computer Science of the University of Manchester 
as part of my MSc dissertation under the supervision of Professor John A. Keane (john.keane@manchester.ac.uk).
Additional involved staff:
Dr. Firat Tekiner (firat.tekiner@manchester.ac.uk);
Dr. Goran Nenadic (gnenadic@manchester.ac.uk).

The work was partially supported by an IBM Faculty Award (Big Data Engineering).

Currently this project supports classification/regression (any Weka Classifier) training and evaluation, Canopy Clustering 
and Association Rule Learning.

The classification/reggresion classes were based on the <b>excellent</b> work of Dr. Mark Hall (http://markahall.blogspot.co.uk/2013/10/weka-and-hadoop-part-1.html) 
on porting Weka to Hadoop.
The Association Rule Learning classes are based on a MapReduce implementation of the Partition algorithm 
(R. Agrawal,J. C. Shafer, "Parallel mining of association rules." IEEE Transactions
on knowledge and Data Engineering 8, no. 6,p 962-969, 1996.)

The Kryo Serialisers for Weka were taken from https://github.com/vpa1977/stormmoa (a very interesting integration of MOA into Storm).

This project was a proof of concept that aimed to demonstrate the feasibility of using Weka's libraries on
Big Data problems and to study the effects of different caching strategies on Big Data Mining workloads.

During benchmarks on AWS, the system demonstrated an average weak scaling efficiency of 91% on clusters up to 128 cores.
Additionally, it was found to be 2-4 times faster than Weka on Hadoop (more to follow on this).

However, being a proof-of-concept, it requires further work to meet industry standards.

Currently, only <b>CSV</b> files can be processed.

The classifier evaluation classes have been successfully ported and executed on top of <b>Spark Streaming</b>. I will upload the code in a separate repository.

This is a Maven project. Building with Maven (goal: package) would yield a deployable uber-jar that 
can be directly executed on a Spark cluster.
 
Execution
========

Copy the uber-jar to the cluster's master node and execute the following:

bin/spark-submit --master (master-node) \ <br>
 --class uk.ac.manchester.ariskk.distributedWekaSpark.main.distributedWekaSpark \ <br>
 --executor-memory (per-node-memory-to-use) \ <br>
  (other spark parameters e.g. number of cores etc) <br>
/path/to/distributedWekaSpark-0.1.0-SNAPSHOT.jar \ <br>
(application parameters)

The parameters are structured as follows:
-parameter1-name value -parameter2-name value etc. The order is irrelevant

Full details on the supported parameters as well as examples of usage can be found in the <b>wiki</b> section of the repository.

I will try to add unit tests, Travis, Logging and error handling as soon as I have the time.


More to follow..





