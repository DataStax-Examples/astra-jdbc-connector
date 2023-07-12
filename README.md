# astra-jdbc-wrapper

This is repository is a placeholder to build a FAT JAR for ing driver. This fat jar could be reused in multiple integrations with JDBC Tools.


## Using Astra Jdbc Driver

| Property                       | Value                                                                       
|:-------------------------------|:----------------------------------------------------------------------------|
| ClassName                      | `com.datastax.astra.jdbc.AstraJdbcDriver`                                   |
| URL with Token                 | `jdbc:astra://<db_name>/<keyspace>?token=<token>`                           |
| URL with user and password     | `jdbc:astra://<db_name>/<keyspace>?user=token&password=<token>`             |
| URL with user and password (2) | `jdbc:astra://<db_name>/<keyspace>?user=<clientId>&password=<clientSecret>` |


## Using Default ING Driver

| Property | Value 
|:----|:------|
| ClassName | `com.ing.data.cassandra.jdbc.CassandraDriver` |
| URL | `jdbc:cassandra://dbaas/${ASTRA_KEYSPACE}?consistency=LOCAL_QUORUM&user=token&password=${ASTRA_TOPEN}&secureconnectbundle=${ASTRA_SCB_PATH}` |


