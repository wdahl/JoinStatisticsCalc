# JoinStatisticsCalc
Java implementation to compute the estimtaed size of a join in a data base

## Run 
Code taks in two command line arguemnts, the tables in the database to join
Code can be run in the terminal using:
```bash
javac joinStats.java
java joinStats [table1] [table2]
```

## Database connection
You will have to change the information used to connect to the db you are trying to connect to. 
You will need to set the url, username, and password you want to use to connect to the database

### URL
```java
String url = "jdbc:postgresql://localhost/[database name]"
```

### Username
```java
props.setProperty("user","[Your user name]");
```

### Password
```java
props.setProperty("password","[Your Password]");
```

## Output
The Output of the code is the estemated join size, the actual join size, and the estimated error
