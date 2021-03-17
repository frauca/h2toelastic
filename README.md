I have made this project to load from the database of bank movements from h2 to elastic search

to run this one compile the project and run

```shell
 java -cp . -jar target/h2toelastic-0.0.1-SNAPSHOT.jar sqlfile/to/query/h2.sql 10000
```

I have also made it possible to import a csv file from fintonic. To run that

```shell
 java -cp . -Dspring.profiles.active=fintonic -jar target/h2toelastic-0.0.1-SNAPSHOT.jar location/of/csv/file 10000
```