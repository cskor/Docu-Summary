#!/bin/bash

gradle build
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/Docu-Summary/middleA
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/Docu-Summary/middleB
$HADOOP_HOME/bin/hadoop fs -rm -r /cs435/Docu-Summary/cacheLarge
$HADOOP_HOME/bin/hadoop jar build/libs/Docu-Summary.jar cs435.hadoop.ProfileA.ProfileADriver /cs435/Docu-Summary/input/demo /cs435/Docu-Summary/middleA  /cs435/Docu-Summary/middleB /cs435/Docu-Summary/cacheLarge
echo Finished Profile A!

