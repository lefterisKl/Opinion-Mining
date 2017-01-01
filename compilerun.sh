#!/bin/sh

sbt package
spark-submit \
  --class "OpinionMining" \
  --master local[8] \
	--driver-cores 8 \
	--executor-memory 4g \
  target/scala-2.11/opinion-mining_2.11-1.0.jar xxx yyy

#replace these
#xxx: path to data
#yyy: path to project folder 
