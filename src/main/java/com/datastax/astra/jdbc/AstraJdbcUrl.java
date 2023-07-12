package com.datastax.astra.jdbc;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Parsing URL as a bean.
 */
public class AstraJdbcUrl implements Serializable {

    /**
     * Beginning for the URL
     */
    public static final String URL_START = "jdbc:astra://";

    /**
     * Enforce Properties Key in URL
     */
    public enum Keys {
        CONSISTENCY_LEVEL("consistency-level"),
        DEBUG("debug"),
        USER("user"),
        TOKEN("token"),
        PASSWORD("password"),
        REGION("region"),
        REQUEST_TIMEOUT("request-timeout");
        private final String key;
        Keys(String key) {
            this.key = key;
        }
        public String getKey() {
            return key;
        }
    }

    /**
     * Database identifier (required)
     */
    private final String databaseId;

    /**
     * Target Keyspace (required)
     */
    private final String keyspace;

    /**
     * Credentials
     */
    private String token;

    /**
     * Credentials
     */
    private String user;

    /**
     * Credentials
     */
    private String password;

    /**
     * Optional region (when not unique)
     */
    private String region;

    /**
     * Optional Consistency Level
      */
    private ConsistencyLevel consistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM;

    /**
     * If Debut Enabled Debug mode show logs
     */
    private boolean debug = false;

    /**
     * Request Timeout
     */
    private int requestTimeout = 10000;

    @Override
    public int hashCode() {
        return Objects.hash(databaseId, region, consistencyLevel, keyspace, token, debug, requestTimeout);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AstraJdbcUrl that = (AstraJdbcUrl) o;
        return debug == that.debug && requestTimeout == that.requestTimeout && Objects.equals(databaseId, that.databaseId) && Objects.equals(region, that.region) && Objects.equals(consistencyLevel, that.consistencyLevel) && Objects.equals(keyspace, that.keyspace) && Objects.equals(token, that.token);
    }

    /**
     * Build URL.
     *
     * @param url
     *      jdbc URL
     */
    public AstraJdbcUrl(String url) throws SQLException {
        this(url, null);
    }

    /**
     * Build URL.
     *
     * @param url
     *      jdbc URL
     * @param properties
     *      optional Properties for arguments
     */
    public AstraJdbcUrl(String url, Properties properties)
    throws SQLException {
        // Beginning of Url validation jdbc:astra://<database-id>/<keyspace>
        if (url!= null && url.startsWith(URL_START)) {
            url = url.replaceAll(URL_START, "");
        } else {
            throw new SQLException("Astra Jdbc URL must start with " + URL_START);
        }
        databaseId = url.substring(0, url.indexOf("/"));
        if (databaseId.isEmpty()) {
            throw new SQLException("Astra Jdbc URL must contain a database identifier " +
                    URL_START + "<database>/<keyspace>");
        }
        if (!url.contains("?")) {
            keyspace = url.substring(url.indexOf("/") + 1);
        } else {
            keyspace = url.substring(url.indexOf("/") + 1, url.indexOf("?"));
        }
        if (keyspace.isEmpty()) {
            throw new SQLException("Astra Jdbc URL must contain a keyspace " +
                    URL_START + "<database>/<keyspace>");
        }
        // Parsing Url Parameters (if Any)
        if (url.contains("?")) {
            String params = url.substring(url.indexOf("?") + 1);
            String[] paramsArray = params.split("&");
            for (String param : paramsArray) {
                String[] keyValue = param.split("=");
                switch (Keys.valueOf(keyValue[0].toUpperCase())) {
                    case REGION:
                        this.region = keyValue[1];
                        break;
                    case CONSISTENCY_LEVEL:
                        consistencyLevel = DefaultConsistencyLevel.valueOf(keyValue[1]);
                        break;
                    case TOKEN:
                        this.token = keyValue[1];
                        break;
                    case DEBUG:
                        debug = Boolean.parseBoolean(keyValue[1]);
                        break;
                    case REQUEST_TIMEOUT:
                        requestTimeout = Integer.parseInt(keyValue[1]);
                        break;
                    case USER:
                        user = keyValue[1];
                        break;
                    case PASSWORD:
                        password = keyValue[1];
                        break;
                    default:
                        throw new SQLException("Unknown parameter " + keyValue[0]);
                }
            }
        }
        if (properties != null) {
            // More likely to have those 2
            if (properties.containsKey(Keys.USER.getKey())) {
                user = properties.getProperty(Keys.USER.getKey());
            }
            if (properties.containsKey(Keys.PASSWORD.getKey())) {
                password = properties.getProperty(Keys.PASSWORD.getKey());
            }
            // Cherry on the cake
            if (properties.containsKey(Keys.REGION.getKey())) {
                region = properties.getProperty(Keys.REGION.getKey());
            }
            if (properties.containsKey(Keys.TOKEN.getKey())) {
                token = properties.getProperty(Keys.TOKEN.getKey());
            }
            if (properties.containsKey(Keys.DEBUG.getKey())) {
                debug = Boolean.parseBoolean(properties.getProperty(Keys.DEBUG.getKey()));
            }
            if (properties.containsKey(Keys.REQUEST_TIMEOUT.getKey())) {
                requestTimeout = Integer.parseInt(properties.getProperty(Keys.REQUEST_TIMEOUT.getKey()));
            }
            if (properties.containsKey(Keys.CONSISTENCY_LEVEL.getKey())) {
                consistencyLevel = DefaultConsistencyLevel.valueOf(
                        properties.getProperty(Keys.CONSISTENCY_LEVEL.getKey(),
                                ConsistencyLevel.LOCAL_QUORUM.name()));
            }
        }
        if (token == null && password != null) {
            this.token = password;
        }
        if (token == null && user == null && password == null) {
            throw new SQLException("Astra Jdbc URL must contain a token or user/password");
        }
    }

    /**
     * Generate Jdbc Connection URL.
     * @return
     *      url
     */
    public String toUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(URL_START);
        sb.append(databaseId);
        sb.append("/");
        sb.append(keyspace);
        sb.append("?");
        if (token != null) {
            sb.append(Keys.TOKEN.getKey());
            sb.append("=");
            sb.append(token);
            sb.append("&");
        }
        if (region != null) {
            sb.append(Keys.REGION.getKey());
            sb.append("=");
            sb.append(region);
            sb.append("&");
        }
        if (consistencyLevel != null) {
            sb.append(Keys.CONSISTENCY_LEVEL.getKey());
            sb.append("=");
            sb.append(consistencyLevel);
            sb.append("&");
        }
        if (user != null) {
            sb.append(Keys.USER.getKey());
            sb.append("=");
            sb.append(user);
            sb.append("&");
        }
        if (password != null) {
            sb.append(Keys.PASSWORD.getKey());
            sb.append("=");
            sb.append(password);
            sb.append("&");
        }
        sb.append("debug=");
        sb.append(Keys.DEBUG.getKey());
        sb.append(debug);
        sb.append("&");
        sb.append(Keys.REQUEST_TIMEOUT.getKey());
        sb.append("=");
        sb.append(requestTimeout);
        return sb.toString();
    }

    /**
     * Gets databaseId
     *
     * @return value of databaseId
     */
    public String getDatabaseId() {
        return databaseId;
    }

    /**
     * Gets region
     *
     * @return value of region
     */
    public Optional<String> getRegion() {
        return Optional.ofNullable(region);
    }

    /**
     * Gets keyspace
     *
     * @return value of keyspace
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Gets token
     *
     * @return value of token
     */
    public String getToken() {
        return token;
    }

    /**
     * Gets debug
     *
     * @return value of debug
     */
    public boolean isDebug() {
        return debug;
    }

    /**
     * Gets requestTimeout
     *
     * @return value of requestTimeout
     */
    public int getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Gets consistencyLevel
     *
     * @return value of consistencyLevel
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
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
}
