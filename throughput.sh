#!/bin/bash

if [ $# -ne 1 ]; then
  echo "./throughput.sh log_file"
  exit 1
fi

java -cp ./flink-benchmarks/target/flink-benchmarks-0.1.0.jar flink.benchmark.utils.AnalyzeTool $1
