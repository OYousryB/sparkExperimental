#!/usr/bin/env bash

source scripts/env.txt

for SF in $SCALES
do
    OUTPUT=$EXP_HOME/$BENCHMARK/$SF

    mkdir -p $EXP_HOME/logs

    if [ $ENABLE_JFR_PROFILING -eq 1 ]
    then
        mkdir -p $EXP_HOME/jfr/$BENCHMARK/driver
        mkdir -p $EXP_HOME/jfr/$BENCHMARK/executor
    fi

    mkdir -p $EXP_HOME/logs/$BENCHMARK

    cleanup()
    {
        if [[ $MASTER != local* ]]
        then
          echo "Shutting down Spark"
          $SPARK_HOME/sbin/stop-master.sh >> $EXP_HOME/logs/run-$BENCHMARK.out
          $SPARK_HOME/sbin/stop-slave.sh >> $EXP_HOME/logs/run-$BENCHMARK.out

          echo "Starting up Spark"
          $SPARK_HOME/sbin/start-master.sh >> $EXP_HOME/logs/run-$BENCHMARK.out
          $SPARK_HOME/sbin/start-slave.sh "spark://$(hostname):7077" >> $EXP_HOME/logs/run-$BENCHMARK.out
        fi
    }

    cleanup
    echo "Benchmarking $BENCHMARK on Spark using '$BENCHMARK_CLASS $BENCHMARK_ARGS'"
    $SPARK_HOME/bin/spark-submit --class $BENCHMARK_CLASS \
        --master $MASTER \
        --total-executor-cores $TOTAL_EXECUTOR_CORES \
        --conf spark.executor.cores=$CORES_PER_EXECUTOR \
        --conf spark.driver.memory=$DRIVER_MEMORY \
        --conf spark.executor.memory=$MEMORY_PER_EXECUTOR \
        --conf spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS \
        --conf spark.local.dir=$SPARK_LOCAL_DIR \
        $JAR_PATH \
        $BENCHMARK_ARGS > $EXP_HOME/logs/$BENCHMARK/s$SF-c$TOTAL_EXECUTOR_CORES-spark.txt
done

echo "Spark Result"
grep $BENCHMARK_RESULT_SPARK $EXP_HOME/logs/$BENCHMARK/*-c$TOTAL_EXECUTOR_CORES-spark.txt