# lsds2020.lab3.grpP202.team09
lsds2020.lab3.grpP202,team09
Lab3 of the course large scale distributed systems

_Members: Marçal Moner NIA 183749 Ivan Martinez NIA 206638 Daniel Gonzalez NIA 184702_

**1 Introduction**

**2 Running example application locally**
- Runs with this comand:
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterStreamingExample ./target/lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties
```

**3 Stateless: joining a static RDD with a real time stream**
- Runs with this comand:
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterStateless ./target/lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties ./src/main/resources/map.tsv
```
**4 Spark Stateful transformations with windows**
- Runs with this comand:
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterWithWindow ./target/lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties ./src/main/resources/map.tsv
```

**5 Spark Stateful transformations with state variables**
- Runs with this comand:
```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterWithState ./target/lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties es
```


**6 DynamoDB**

**6.1 Writing to Dynamo DB**
- Runs with this comand:

```
spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$PWD/src/main/resources/log4j.properties --class upf.edu.TwitterHashtags ./target/lab3-1.0-SNAPSHOT.jar ./src/main/resources/application.properties
```

**6.2 Reading from DynamoDB**
- Runs with this comand:
```
none
```

