package com.datastax.astra.jdbc.jdbc;

import com.datastax.astra.jdbc.AstraJdbcDataSource;
import com.datastax.astra.jdbc.AstraJdbcUrl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class TestConnectivity {

    public static final String TOKEN = "<change_me>";
    public static final String KEYSPACE = "beam";
    public static final String DB_ID = "beam_sdk_integration_test";

    //@Test
    public void shouldConnectWithJdbcCassandra() {
        String url = "jdbc:cassandra://dbaas/" + KEYSPACE + "?user=token"
                + "&password=" + TOKEN
                + "&secureconnectbundle=/Users/cedricklunven/Downloads/beam_integration_test_scb.zip";
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(url)) {
            System.out.println("[OK] Welcome to ASTRA. Connected to Keyspace "+  connection.getCatalog());
            PreparedStatement ps = connection.prepareStatement("select * from scientist limit 10;");
            java.sql.ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt("person_id"));
            }
        } catch (java.sql.SQLException e) {
            System.out.println("[KO] " + e.getMessage());
        }
    }

    //@Test
    public void shouldConnectWithJdbcAstraUrl() {
        String url = "jdbc:astra://" + DB_ID + "/" + KEYSPACE + "?user=token&password=" + TOKEN;
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(url)) {
            showDb(connection);
        } catch (java.sql.SQLException e) {
            System.out.println("[KO] " + e.getMessage());
        }
    }

    //@Test
    public void shouldConnectWithJdbcAstraProperties() {
        String url = "jdbc:astra://" + DB_ID + "/" + KEYSPACE;
        Properties props = new Properties();
        props.setProperty(AstraJdbcUrl.Keys.USER.getKey(), "token");
        props.setProperty(AstraJdbcUrl.Keys.PASSWORD.getKey(), TOKEN);
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(url, props)) {
            showDb(connection);
        } catch (java.sql.SQLException e) {
            System.out.println("[KO] " + e.getMessage());
        }
    }

    //@Test
    public void shouldConnectWithDataSource() {
        AstraJdbcDataSource ds = new AstraJdbcDataSource("token", TOKEN, DB_ID, KEYSPACE);
        try (java.sql.Connection connection = ds.getConnection()) {
            showDb(connection);
        } catch (java.sql.SQLException e) {
            System.out.println("[KO] " + e.getMessage());
        }
    }

    private void showDb(java.sql.Connection connection) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("select * from scientist limit 50;");
        java.sql.ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString("person_name") + " - " + rs.getInt("person_id"));
        }
    }

}
