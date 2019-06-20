#!/usr/bin/env bash

source scripts/env.txt

for SF in $SCALES
do
    OUTPUT=$DB/$BENCHMARK/$SF

    mkdir -p $EXP_HOME/logs/gen/$BENCHMARK

      echo "Shutting down Spark"
      $SPARK_HOME/sbin/stop-master.sh >> $EXP_HOME/logs/run-$BENCHMARK.out
      $SPARK_HOME/sbin/stop-slave.sh >> $EXP_HOME/logs/run-$BENCHMARK.out

      echo "Starting up Spark"
      $SPARK_HOME/sbin/start-master.sh >> $EXP_HOME/logs/run-$BENCHMARK.out
      $SPARK_HOME/sbin/start-slave.sh "spark://$(hostname):7077" >> $EXP_HOME/logs/run-$BENCHMARK.out

    echo "Parquet Generation for scale factor $SF"
    if [[ ! -d $PARQUET_OP_DIR ]]
    then
        # Convert into Parquet ...............................
        rm -rf $PARQUET_OP_DIR
        mkdir -p $PARQUET_OP_DIR

        $SPARK_HOME/bin/spark-submit \
            --class benchmark.$BENCHMARK.gen.ParquetGen \
            --master $MASTER \
            --total-executor-cores $TOTAL_EXECUTOR_CORES \
            --conf spark.executor.cores=$CORES_PER_EXECUTOR \
            --conf spark.driver.memory=$DRIVER_MEMORY \
            --conf spark.executor.memory=$MEMORY_PER_EXECUTOR \
            --conf spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS \
            --conf spark.local.dir=$SPARK_LOCAL_DIR \
            $JAR_PATH \
            $TBL_DIR $PARQUET_OP_DIR $PARQUET_PARTITIONS > $EXP_HOME/logs/gen/$BENCHMARK/s$SF-parquet.txt
    fi
done
