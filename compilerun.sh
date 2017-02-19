#!/bin/sh

sbt package
spark-submit \
  --class "OpinionMining" \
  --master local[*] \
	--driver-memory 4g \
  target/scala-2.11/opinion-mining_2.11-1.0.jar \
	dataFolder projectFolder 4 mode output
 

#dataFolder projectFolder partitions mode output
#replace these with
#	arg0. dataFolder: path to  data directory (should contain /train/pos, /train/neg and /test) 
#	arg1. projectFolder: path to project folder
# arg2. partitions: number of partitions to use on data RDDs, already set to 4. (4 was fast for local[*] executions)
#	arg3. mode: set to EVALUATE for model evaluation with hold-out (svm,naive bayes, log regression) or PREDICT to classify the reviews in /test with our best model.
#	arg4. output: path to file where predictions will be saved (if you choose PREDICT mode). The file should not exist prior to execution to avoid Exception org.apache.hadoop.mapred.FileAlreadyExistsException.

#Example parameters: 
# for evaluation --> /home/lefteris/data /home/lefteris/Desktop/Opinion-Mining 4 EVALUATE
# for prediction --> /home/lefteris/data /home/lefteris/Desktop/Opinion-Mining 4 PREDICT /home/lefteris/predictResults
