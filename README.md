# lsds2020.lab3.grpP202.team09
lsds2020.lab3.grpP202,team09
Lab3 of the course large scale distributed systems

_Members: Marçal Moner NIA 183749 Ivan Martinez NIA 206638 Daniel Gonzalez NIA 184702_


**1 era introduccion**

**el ejemplo del punto 2 se ejecuta con el comando:**
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterStreamingExample lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties
```

**3 con**
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterStateless lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties ./src/main/resources/map.tsv
```
**4 con**
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterWithWindow ./target/lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties ./src/main/resources/map.tsv
```

**5 con**
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterWithState ./target/lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties es
```
