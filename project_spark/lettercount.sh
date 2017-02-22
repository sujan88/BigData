#!/bin/bash
# $1  add base folder in hadoop        eg :input
# $2 output path        eg : output_stripe
# $3 wordcount limit        eg : 20


#sample command    sh ./lettercount.sh input  outputX 20

localBaseDir=$(pwd)
hadoopBaseDir=/user/cloudera/project_spark_lettercount
hadoopOutputDir=$hadoopBaseDir/$2
hadoopInputDir=$hadoopBaseDir/$1
wordcountlimit=$3

echo "removing old jars created by user. . ."

echo "removing old jar file from local"
rm -r $localBaseDir/lettercount/jars/lettercount.jar
classpath=$localBaseDir/lettercount/target/classes/edu.mum.bigdata.spark.lettercount.LetterCountSpark
s
echo "CREATING JAR FILE $lettercount.jar . . ."
cd $localBaseDir/lettercount/target/classes
jar -cvfm ../../jars/lettercount.jar lettercount.MF *
echo "SUCCESS "

echo "CREATING PROJECT FOLDER . . ."
hadoop fs -rm -r $hadoopBaseDir
hadoop fs -mkdir $hadoopBaseDir
hadoop fs -mkdir $hadoopInputDir
echo "CREATED  FOLDER $hadoopInputDir"


echo "COPYING INPUT FILE customerChoice.txt INTO $hadoopInputDir . . . "
hadoop fs -put $localBaseDir/lettercount/input/log.txt $hadoopInputDir
echo "SUCCESS "

#hadoop fs -rm -r $hadoopOutputDir
#echo "DELETED $hadoopOutputDir FOLDER FROM HADOOP CLUSTER."

echo "RUNNING  lettercount.jar FILE . . . "
spark-submit --master local $localBaseDir/lettercount/jars/lettercount.jar $hadoopInputDir $hadoopOutputDir $wordcountlimit
echo "SUCCESS "


rm -r $localBaseDir/lettercount/output/$2
echo "DELETED $localBaseDir/lettercount/output/$2 FROM LOCAL."

echo "COPYING OUTPUT RESULTS FROM HADOOP TO LOCAL FOLDER /output_$lettercount . . . "
hadoop fs -copyToLocal $hadoopOutputDir $localBaseDir/lettercount/output
echo "SUCCESS"


