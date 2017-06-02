flink run -m yarn-cluster -yn 3 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir small \
--coefficient 0.2 \
--set-size 5

flink run -m yarn-cluster -yn 3 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir test \
--coefficient 0.2 \
--set-size 5

flink run -m yarn-cluster -yn 10 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir large \
--coefficient 0.3 \
--set-size 2