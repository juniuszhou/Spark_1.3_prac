#!/bin/sh
sbtpath="/home/junius/develop/spark-1.1.0"
cd $sbtpath

cmd="/home/junius/develop/spark-1.1.0/bin/run-example org.apache.spark.examples."
cmdwithclass=${cmd}${1}
$cmdwithclass

