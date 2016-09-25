#!/bin/bash

mvn install:install-file -Dfile=/home/efukuda/Projects/ericfukuda/yahoo-streaming-benchmark/protobuf/protobuf-java-3.0.2.jar -DgroupId=com.ericfukuda.flink -DartifactId=coreproto -Dversion=0.1 -Dpackaging=jar -DgeneratePom=true
mvn install:install-file -Dfile=/home/efukuda/Projects/ericfukuda/yahoo-streaming-benchmark/protobuf/myprotobuf.jar -DgroupId=com.ericfukuda.flink -DartifactId=myproto -Dversion=0.1 -Dpackaging=jar -DgeneratePom=true
