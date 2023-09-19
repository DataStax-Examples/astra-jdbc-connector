package com.datastax.astra.jdbc;

import com.datastax.astra.jdbc.exceptions.AstraJdbcNotSupportedOperation;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.dtsx.astra.sdk.db.AstraDbClient;
import com.dtsx.astra.sdk.db.DatabaseClient;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.utils.DriverUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.getDriverProperty;

/**
 * This Class would wrap the DataStax Java Driver for Apache Cassandra.
 * We except a URL with this form:
 * jdbc:astra://{database-id}/{keyspace}?token=AstraCS:....&region={database-region}
 */
public class AstraJdbcDriver implements java.sql.Driver {

    /**
     * Work with CQL and Astra.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AstraJdbcDriver.class);

    static {
        try {
            final AstraJdbcDriver driverInstance = new AstraJdbcDriver();
            DriverManager.registerDriver(driverInstance);
            LOGGER.info("AstraJdbcDriver registered to the Jdbc Driver Manager.");
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Reuse Session when possible.
     */
    final LoadingCache<AstraJdbcUrl, CqlSession > cachedSessions = Caffeine.newBuilder()
            .build(jdbcUrl -> {
                LOGGER.info("Creating a new Session for db '" + jdbcUrl.getDatabaseId() + "'");
                return buildSession(jdbcUrl);
            });

    public static void register() {}

    public static CqlSession buildSession(AstraJdbcUrl jdbcUrl) {
        AstraDbClient astraDbClient = new AstraDbClient(jdbcUrl.getToken());
        // First Test with the Name
        DatabaseClient dbClient;
        long count = astraDbClient.findByName(jdbcUrl.getDatabaseId()).count();
        if (count == 1) {
            LOGGER.debug("Found Database by Name");
            dbClient = astraDbClient.databaseByName(jdbcUrl.getDatabaseId());
        } else if (count > 1) {
            throw new RuntimeException("Found more than one database with the same name");
        } else {
            LOGGER.debug("Found Database by Id");
            dbClient = astraDbClient.database(jdbcUrl.getDatabaseId());
        }
        byte[] scb = dbClient.downloadDefaultSecureConnectBundle();
        CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .withKeyspace(jdbcUrl.getKeyspace())
                .withCloudSecureConnectBundle(new ByteArrayInputStream(scb));
        if (jdbcUrl.getUser() != null) {
            sessionBuilder = sessionBuilder.withAuthCredentials(jdbcUrl.getUser(), jdbcUrl.getPassword());
        } else {
            sessionBuilder = sessionBuilder.withAuthCredentials("token", jdbcUrl.getToken());
        }
        return sessionBuilder.build();
    }

    /**
     * Creates a new connection to the database.
     * @param url the URL of the database to which to connect
     * @param properties a list of arbitrary string tag/value pairs as
     * connection arguments. Normally at least a "user" and
     * "password" property should be included.
     *
     * @return
     *      a <code>Connection</code> object that represents a
     * @throws SQLException
     *     if a database access error occurs
     */
    public Connection connect(String url, Properties properties) throws SQLException {
        AstraJdbcUrl jdbcUrl = new AstraJdbcUrl(url, properties);
        return new CassandraConnection(this.cachedSessions.get(jdbcUrl),
                jdbcUrl.getKeyspace(),
                jdbcUrl.getConsistencyLevel(),
                jdbcUrl.isDebug(),
               null);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
        AstraJdbcUrl jdbcUrl = new AstraJdbcUrl(url, properties);
        List<DriverPropertyInfo> propertyInfoList = new ArrayList<>();

        DriverPropertyInfo token = new DriverPropertyInfo(AstraJdbcUrl.Keys.TOKEN.getKey(), jdbcUrl.getToken());
        token.description = "Token stands as password when could client-id / client-secret is not provided";
        token.required = true;
        propertyInfoList.add(token);

        // TODO process all variables
        return propertyInfoList.toArray(new DriverPropertyInfo[0]);
    }

    /** {@inheritDoc} */
    @Override
    public boolean acceptsURL(final String url) {
        return url.startsWith(AstraJdbcUrl.URL_START);
    }

    /** {@inheritDoc} */
    @Override
    public int getMajorVersion() {
        return DriverUtil.parseVersion(getDriverProperty("driver.version"), 0);
    }

    /** {@inheritDoc} */
    @Override
    public int getMinorVersion() {
        return DriverUtil.parseVersion(getDriverProperty("driver.version"), 1);
    }

    /** {@inheritDoc} */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public java.util.logging.Logger getParentLogger()
    throws SQLFeatureNotSupportedException {
        throw new AstraJdbcNotSupportedOperation("Driver.getParentLogger()");
    }

}
