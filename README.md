Sample Java-Spark Program
===
This is a standalone Maven-Java program.

### To compile

````mvn clean compile````

### To execute

````mvn exec:java -Dexec.mainClass="com.japharr.sample;.Main"````


### [Main](https://github.com/japharr/spark-java-sample/blob/main/src/main/java/com/japharr/sample/Main.java)
This demonstrate basic Spark operation on a list of integer as input source

### [Main2](https://github.com/japharr/spark-java-sample/blob/main/src/main/java/com/japharr/sample/Main2.java)
This demonstrate reading a whole file from an HDFS directory

### [Main3](https://github.com/japharr/spark-java-sample/blob/main/src/main/java/com/japharr/sample/Main3.java)
This demonstrate reading a file line by line from an HDFS directory and printing it out on a console
