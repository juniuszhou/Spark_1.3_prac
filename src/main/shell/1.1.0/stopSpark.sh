#!/bin/sh

# exec sbt to get example package
sbtpath="/home/junius/develop/spark-1.0.0"
cd $sbtpath

# start all spark service
sbin/stop-all.sh

