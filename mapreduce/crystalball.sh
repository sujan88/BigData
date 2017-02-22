#!/bin/bash
# $1  add base folder in hadoop        eg :input
# $2 strie or pair or hybrid         eg : output_stripe
# $3 strie or pair or hybrid         eg : stripe


#sample command    sh ./crystalball.sh input  outputX pair

localBaseDir=$(pwd)
hadoopBaseDir=/user/cloudera/project_relative_frequency
hadoopOutputDir=$hadoopBaseDir/$2
jobName=$3

hadoopInputDir=$hadoopBaseDir/$1

echo "removing old jars created by user. . ."

echo "removing old jar file from local"
rm -r $localBaseDir/crystalball/jars/$jobName.jar

echo "CREATING JAR FILE $jobName.jar . . ."
cd $localBaseDir/crystalball/bin
jar -cvfm ../jars/$jobName.jar $jobName.MF *
echo "SUCCESS "

echo "CREATING PROJECT FOLDER . . ."
hadoop fs -rm -r $hadoopBaseDir
hadoop fs -mkdir $hadoopBaseDir
hadoop fs -mkdir $hadoopInputDir
echo "CREATED  FOLDER $hadoopInputDir"


echo "COPYING INPUT FILE customerChoice.txt INTO $hadoopInputDir . . . "
hadoop fs -put $localBaseDir/crystalball/input/customerChoice.txt $hadoopInputDir
echo "SUCCESS "

#hadoop fs -rm -r $hadoopOutputDir
#echo "DELETED $hadoopOutputDir FOLDER FROM HADOOP CLUSTER."

echo "RUNNING  $jobName.jar FILE . . . "
hadoop jar $localBaseDir/crystalball/jars/$jobName.jar $hadoopInputDir $hadoopOutputDir
echo "SUCCESS "


rm -r $localBaseDir/crystalball/output/$2
echo "DELETED $localBaseDir/crystalball/output/$2 FROM LOCAL."

echo "COPYING OUTPUT RESULTS FROM HADOOP TO LOCAL FOLDER /output_$jobName . . . "
hadoop fs -copyToLocal $hadoopOutputDir $localBaseDir/crystalball/output
echo "SUCCESS"


