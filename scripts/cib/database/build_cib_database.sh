#!/usr/bin/env bash

export SPARK_HOME=/usr/hdp/current/spark2-client
export PATH="$SPARK_HOME/bin:$PATH"
export PYTHONPATH=wczip

cd src
zip -r ../dist/worldcereal.zip worldcereal
cd ..

################
# SETTINGS
################

CIB='CIB_V1'
ROOT='/data/worldcereal/cib'

################

PYSPARK_PYTHON=./ewocenv/bin/python \
${SPARK_HOME}/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="./ewocenv/bin/python" \
--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON="./ewocenv/bin/python" \
--conf spark.executorEnv.LD_LIBRARY_PATH="./ewocenv/lib" \
--conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH="./ewocenv/lib" \
--conf spark.executorEnv.PYSPARK_PYTHON="./ewocenv/bin/python" \
--conf spark.yarn.appMasterEnv.PROJ_LIB="./ewocenv/share/proj" \
--conf spark.executorEnv.PROJ_LIB="./ewocenv/share/proj" \
--conf spark.yarn.appMasterEnv.XDG_CACHE_HOME=.cache \
--conf spark.executorEnv.XDG_CACHE_HOME=.cache \
--executor-memory 1g --driver-memory 6g \
--conf spark.executorEnv.GDAL_CACHEMAX=512 \
--conf spark.network.timeout=600s \
--conf spark.driver.memoryOverhead=8g --conf spark.executor.memoryOverhead=3g \
--conf spark.driver.maxResultSize=0 \
--conf spark.memory.fraction=0.2 \
--conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=2000 \
--master yarn --deploy-mode cluster --queue default \
--conf spark.speculation=true \
--conf spark.app.name="CIB-DATABASE" \
--py-files /data/worldcereal/software/wheels/satio-1.1.11-py3-none-any.whl \
--archives "dist/worldcereal.zip#wczip","hdfs:///tapdata/worldcereal/worldcereal_gdal3.tar.gz#ewocenv" \
scripts/cib/database/build_cib_database.py \
--cib ${CIB} \
--cibroot ${ROOT} \
--spark
