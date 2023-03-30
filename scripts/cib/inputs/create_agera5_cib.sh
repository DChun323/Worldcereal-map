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
DRIVERMEM='6g'
EXMEM='1g'
# FOR MEDIUM LPIS LIKE SIGPAC Catalunya:
# DRIVERMEM='10g'
# EXMEM='6g'
# FOR LARGE LPIS LIKE FRANCE:
# DRIVERMEM='16g'
# EXMEM='16g'
################

PYSPARK_PYTHON=./ewocenv/bin/python \
${SPARK_HOME}/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="./ewocenv/bin/python" \
--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON="./ewocenv/bin/python" \
--conf spark.executorEnv.LD_LIBRARY_PATH="./ewocenv/lib" \
--conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH="./ewocenv/lib" \
--conf spark.executorEnv.PYSPARK_PYTHON="./ewocenv/bin/python" \
--conf spark.yarn.appMasterEnv.XDG_CACHE_HOME=.cache \
--conf spark.executorEnv.XDG_CACHE_HOME=.cache \
--conf spark.yarn.am.waitTime=500s \
--executor-memory 4g --driver-memory 4g \
--conf spark.executorEnv.GDAL_CACHEMAX=512 \
--conf spark.driver.memoryOverhead=${DRIVERMEM} --conf spark.executor.memoryOverhead=${EXMEM} \
--conf spark.memory.fraction=0.2 \
--conf spark.shuffle.service.enabled=false --conf spark.dynamicAllocation.enabled=false \
--num-executors 100 \
--master yarn --deploy-mode cluster --queue default \
--conf spark.speculation=false \
--conf spark.app.name="CIB-AGERA5" \
--py-files /data/worldcereal/software/wheels/satio-1.1.11-py3-none-any.whl \
--archives "dist/worldcereal.zip#wczip","hdfs:///tapdata/worldcereal/worldcereal_gdal3.tar.gz#ewocenv" \
scripts/cib/inputs/create_agera5_cib.py