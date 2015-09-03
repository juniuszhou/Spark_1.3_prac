#!/bin/sh

# exec sbt to get example package
sbtpath="/home/junius/develop/spark-1.1.0"
cd $sbtpath
sbt/sbt -mem 1024m assembly

