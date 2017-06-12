#!/usr/bin/env bash

# Task 1
flink run -m yarn-cluster -yn 10 \
-c ml.Task1 \
target/ml-1.0-SNAPSHOT.jar \
--dir small

flink run -m yarn-cluster -yn 10 \
-c ml.Task1 \
target/ml-1.0-SNAPSHOT.jar \
--dir large

# Task 2
flink run -m yarn-cluster -yn 10 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir test \
--coefficient 0.3 \
--set-size 5

flink run -m yarn-cluster -yn 10 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir small \
--coefficient 0.3 \
--set-size 4

flink run -m yarn-cluster -yn 100 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir large \
--coefficient 0.3 \
--set-size 4

# Task 3
flink run -m yarn-cluster -yn 10 \
-c ml.Task3 \
target/ml-1.0-SNAPSHOT.jar \
--dir test \
--threshold 0.6

flink run -m yarn-cluster -yn 10 \
-c ml.Task3 \
target/ml-1.0-SNAPSHOT.jar \
--dir small \
--threshold 0.6

flink run -m yarn-cluster -yn 10 \
-c ml.Task3 \
target/ml-1.0-SNAPSHOT.jar \
--dir large \
--threshold 0.6