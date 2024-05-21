#!/bin/bash

job=$1

if [[ "$(echo "$job" | tr '[:upper:]' '[:lower:]')" == *"train"* ]]; then
  type="train.py"
elif [[ "$(echo "$job" | tr '[:upper:]' '[:lower:]')" == *"infer"* ]]; then
  type="inference.py"
elif [[ "$(echo "$job" | tr '[:upper:]' '[:lower:]')" == *"feature"* ]]; then
  type="features.py"
fi



export PYTHONIOENCODING=utf8
export HADOOP_USER_NAME=hdfs
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hbase/conf
export PYSPARK_PYTHON=./venv/bin/python

log4j_setting="-Dlog4j.configuration=file:log4j.properties"

zip -j src.zip src/*.py

spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python \
             --master yarn \
             --deploy-mode client \
             --queue analytics \
             --num-executors 6  \
             --driver-memory 16g \
             --executor-memory 16g \
             --conf "spark.driver.extraJavaOptions=${log4j_setting}" \
             --conf "spark.executor.extraJavaOptions=${log4j_setting}" \
             --files log4j.properties \
             --conf spark.yarn.dist.files=./hive-site.xml \
             --py-files "h2o_pysparkling_2.4-3.36.1.2-1-2.4.zip" \
             --py-files src.zip \
             $type
