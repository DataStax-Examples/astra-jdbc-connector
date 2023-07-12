package com.datastax.astra.jdbc;

import com.datastax.astra.jdbc.exceptions.AstraJdbcNotSupportedOperation;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.PooledCassandraConnection;

import javax.sql.ConnectionPoolDataSource;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class AstraJdbcDataSource implements ConnectionPoolDataSource, javax.sql.DataSource {

    static {
        try {
            Class.forName("com.datastax.astra.jdbc.AstraJdbcDriver");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    private final String user;
    private final String password;
    private final String database;
    private final String keyspace;
    private String region;
    private String consistencyLevel;
    private Integer requestTimeout;

    public AstraJdbcDataSource(String user, String password, String database, String keyspace) {
        this.user = user;
        this.password = password;
        this.database = database;
        this.keyspace = keyspace;
    }

    public AstraJdbcDataSource(String user, String password, String database, String keyspace,
                               String region, String consistencyLevel, Integer requestTimeout) {
        this(user, password, database, keyspace);
        this.region = region;
        this.consistencyLevel = consistencyLevel;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Gets user
     *
     * @return value of user
     */
    public String getUser() {
        return user;
    }

    /**
     * Gets password
     *
     * @return value of password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Gets database
     *
     * @return value of database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Gets keyspace
     *
     * @return value of keyspace
     */
    public String getKeyspace() {
        return keyspace;
    }

    /** {@inheritDoc} */
    @Override
    public CassandraConnection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    /** {@inheritDoc} */
    @Override
    public CassandraConnection getConnection(String username, String pPassword)
    throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(AstraJdbcUrl.URL_START);
        sb.append(database);
        sb.append("/");
        sb.append(keyspace);
        sb.append("?");
        sb.append(AstraJdbcUrl.Keys.USER.getKey());
        sb.append("=");
        sb.append((username != null) ? username : this.user);
        sb.append("&");
        sb.append(AstraJdbcUrl.Keys.PASSWORD.getKey());
        sb.append("=");
        sb.append((pPassword != null) ? pPassword : this.password);
        sb.append("&");
        if (requestTimeout!= null) {
            sb.append(AstraJdbcUrl.Keys.REQUEST_TIMEOUT.getKey());
            sb.append("=");
            sb.append(requestTimeout);
        }
        if (region!= null) {
            sb.append(AstraJdbcUrl.Keys.REGION.getKey());
            sb.append("=");
            sb.append(region);
        }
        if (consistencyLevel!= null) {
            sb.append(AstraJdbcUrl.Keys.CONSISTENCY_LEVEL.getKey());
            sb.append("=");
            sb.append(consistencyLevel);
        }
        return (CassandraConnection) DriverManager.getConnection(sb.toString(), null);
    }

    /** {@inheritDoc} */
    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    /** {@inheritDoc} */
    @Override
    public PrintWriter getLogWriter() {
        return DriverManager.getLogWriter();
    }

    /** {@inheritDoc} */
    @Override
    public void setLoginTimeout(final int timeout) {
        DriverManager.setLoginTimeout(timeout);
    }

    /** {@inheritDoc} */
    @Override
    public void setLogWriter(final PrintWriter writer) {
        DriverManager.setLogWriter(writer);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isWrapperFor(final Class<?> wrappedClass) {
        return wrappedClass != null && wrappedClass.isAssignableFrom(this.getClass());
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(final Class<T> wrappedClass) throws SQLException {
        if (isWrapperFor(wrappedClass)) {
            return wrappedClass.cast(this);
        } else {
            throw new SQLException(String.format("No object was found that matched the provided interface: %s",
                    wrappedClass.getSimpleName()));
        }
    }

    /** {@inheritDoc} */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new AstraJdbcNotSupportedOperation("DataSource.getParentLogger()");
    }

    /** {@inheritDoc} */
    @Override
    public PooledCassandraConnection getPooledConnection() throws SQLException {
        return new PooledCassandraConnection(getConnection());
    }

    /** {@inheritDoc} */@Override
    public PooledCassandraConnection getPooledConnection(final String user, final String password) throws SQLException {
        return new PooledCassandraConnection(getConnection(user, password));
    }
}
