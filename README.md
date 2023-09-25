# Jdbc Adapter for Astra

[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)

This repository provides a few classes (wrapper) to simplify the configuration of Astra using the [CASSANDRA ING DRIVERS](https://github.com/ing-bank/cassandra-jdbc-wrapper). 

We would like to thank the ING Team and especially [@maximevw](https://github.com/maximevw) for his work on maintaining Cassandra JDBC library.

As for the Cassandra JDBC driver this component is delivered as-is with a Apache2  License. 


## Using Astra Jdbc Driver

| Property                       | Value                                                                       
|:-------------------------------|:----------------------------------------------------------------------------|
| ClassName                      | `com.datastax.astra.jdbc.AstraJdbcDriver`                                   |
| URL with Token                 | `jdbc:astra://<db_name>/<keyspace>?token=<token>`                           |
| URL with user and password     | `jdbc:astra://<db_name>/<keyspace>?user=token&password=<token>`             |
| URL with user and password (2) | `jdbc:astra://<db_name>/<keyspace>?user=<clientId>&password=<clientSecret>` |



## Tutorials

- [Sample Usage with DBeaver](https://awesome-astra.github.io/docs/pages/data/explore/dbeaver/?h=dbea#astra-community-jdbc-drivers)

![image](https://github.com/DataStax-Examples/astra-jdbc-connector/assets/726536/4b8b686a-4a80-4942-b302-42ec9f21b974)


- [Sample Usage with Datagrip](https://awesome-astra.github.io/docs/pages/data/explore/datagrip/?h=datagr#astra-community-jdbc-drivers)

![image](https://github.com/DataStax-Examples/astra-jdbc-connector/assets/726536/1f8f0ead-af22-42d0-8ca2-72c6d0332602)





