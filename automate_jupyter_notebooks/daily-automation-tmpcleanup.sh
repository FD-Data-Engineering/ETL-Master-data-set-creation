#!/usr/bin/env bash
#
# Spark.2.4.5
echo "spark.2.4.5"
export SPARK_HOME=${HOME}/spark/spark-2.4.5-bin-hadoop2.7
export HADOOP_HOME=${SPARK_HOME}
export JAVA_HOME=/usr/lib/jvm/default-java
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS=notebook
export PYSPARK_PYTHON=${HOME}/anaconda3/bin/python
#
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
# Setup IP Spark IP
MYIP=$(hostname -I | cut -d' ' -f1)
echo $MYIP
export SPARK_LOCAL_IP=0.0.0.0
#
source ~/.bashrc
#
### Workarround for Delta Lake format
###
export PACKAGES="io.delta:delta-core_2.11:0.5.0"
export PYSPARK_SUBMIT_ARGS="--packages ${PACKAGES}  pyspark-shell"
###
#
#
HOME=/home/notebookuser
source $HOME/.profile
cd $HOME/notebooks/crontab
DATENB=$(date +'%Y-%m-%d')
NBLOGFILE=$HOME/notebooks/crontab/crontab-run-$DATENB.log
#
rm -rf  /tmp/*
sleep 2
# 
#
rm -rf  /tmp/*
#