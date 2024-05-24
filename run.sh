#!/bin/bash

job=$1
env=${2:-prod}

if [[ "$(echo "$job" | tr '[:upper:]' '[:lower:]')" == *"main"* ]]; then
  type="main.py"
else
  echo "Error: job does not contain 'main'" >&2
  exit 1
fi

if [[ "$(echo "$env" | tr '[:upper:]' '[:lower:]')" == *"dev"* ]]; then
  env="dev"
elif [[ "$(echo "$env" | tr '[:upper:]' '[:lower:]')" == *"prod"* ]]; then
  env="prod"
else
  echo "Error: env does not contain 'dev' or 'prod'" >&2
  exit 1
fi

export PYTHONIOENCODING=utf8
export HADOOP_USER_NAME=hdfs
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=./venv/bin/python

log4j_setting="-Dlog4j.configuration=file:log4j.properties"

zip -j src.zip src/*.py

spark-submit \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --driver-memory 8g \
  --executor-memory 4g \
  --conf "spark.driver.extraJavaOptions=${log4j_setting}" \
  --conf "spark.executor.extraJavaOptions=${log4j_setting}" \
  --files log4j.properties \
  --py-files src.zip \
  $type $env
