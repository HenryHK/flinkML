#!/usr/bin/env bash
flink run -m yarn-cluster -yn 3 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir small \
--coefficient 0.2 \
--set-size 4

flink run -m yarn-cluster -yn 3 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir test \
--coefficient 0.3 \
--set-size 5

flink run -m yarn-cluster -yn 100 \
-c ml.Task2 \
target/ml-1.0-SNAPSHOT.jar \
--dir large \
--coefficient 0.3 \
--set-size 3

flink run -m yarn-cluster -yn 3 \
-c ml.Task2Naive \
target/ml-1.0-SNAPSHOT.jar \
--dir small \
--coefficient 0.2 \
--set-size 4

flink run -m yarn-cluster -yn 3 \
-c ml.Task3 \
target/ml-1.0-SNAPSHOT.jar \
--dir test \
--threshold 0.6

/*****************************/
([308],158)
([363],113)
([945],138)
([985],128)
([229],165)
([928],126)
([820],151)
([562],109)
([923],125)
([763],149)
([949],146)
([353],159)
([943],117)
([978],132)
([962],147)
([926],143)
([470],156)
([869],134)
([902],137)
([975],155)
([43],160)
([915],152)
([967],145)
([815],148)
([925],139)
([891],162)
([441],153)
([649],112)
([873],129)
([947],133)
([63],110)
([64],121)
([920],115)
([686],122)
([964],127)
([958],120)
([922],119)
([656],124)
([883],135)
([829],123)
([955],136)
([655],104)
([909],131)
([858],118)
([42],161)
([976],130)
([657],157)
([953],141)
([990],140)
([257],114)
([968],116)
([806],144)
([547],150)
([333],154)
([845],142)
([317],107)


/**************************************************/