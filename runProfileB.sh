#!/bin/bash

gradle build
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/Docu-Summary/results
$HADOOP_HOME/bin/hadoop jar build/libs/Docu-Summary.jar cs435.hadoop.ProfileB.SentenceDriver /cs435/Docu-Summary/cache /cs435/Docu-Summary/input /cs435/Docu-Summary/results
echo Finished Profile B!
