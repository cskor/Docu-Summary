#!/bin/bash

gradle build
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/Docu-Summary/middle
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/Docu-Summary/results
$HADOOP_HOME/bin/hadoop jar build/libs/Docu-Summary.jar cs435.hadoop.ProfileA.ProfileADriver /cs435/Docu-Summary/input /cs435/Docu-Summary/middle  /cs435/Docu-Summary/results
echo Finished Job A!