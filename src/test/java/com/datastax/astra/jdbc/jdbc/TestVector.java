package com.datastax.astra.jdbc.jdbc;

import com.datastax.astra.jdbc.AstraJdbcDataSource;
import com.dtsx.astra.sdk.db.AstraDbClient;
import com.dtsx.astra.sdk.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

/**
 * This class test Support of Vector with CQL
 */
public class TestVector {

    static Logger logger = LoggerFactory.getLogger(TestVector.class);

    private static final String TEST_DB = "test_jdbc_wrapper";
    private static final String TEST_KEYSPACE = "test";
    private static String token;
    private static AstraJdbcDataSource jdbcDataSource;

    @BeforeAll
    public static void setupDb() throws SQLException {
        token = Optional.ofNullable(System.getenv("ASTRA_DB_APPLICATION_TOKEN"))
                .orElseThrow(() -> new IllegalStateException("Please define env variable ASTRA_DB_APPLICATION_TOKEN"));
        logger.info("[setup] - Token found");

        // Create DB
        TestUtils.setupVectorDatabase(TEST_DB, TEST_KEYSPACE);
        logger.info("[setup] - DB Setup");

        // Create DataSource
        jdbcDataSource = new AstraJdbcDataSource(token, TEST_DB, TEST_KEYSPACE);
        logger.info("[setup] - Jdbc Connection established");

        // Create Tables and data
        createVectorTable();
        logger.info("[setup] - Schema Created");
    }

    @Test
    public void testSimilaritySearch() {
        try (Connection connection = jdbcDataSource.getConnection()) {
            PreparedStatement ps = connection.prepareStatement("" +
                    "SELECT\n" +
                    "     product_id, product_vector,\n" +
                    "     similarity_dot_product(product_vector,[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]) as similarity\n" +
                    "FROM pet_supply_vectors\n" +
                    "ORDER BY product_vector\n" +
                    "ANN OF [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]\n" +
                    "LIMIT 2;");
            java.sql.ResultSet rs = ps.executeQuery();
            // A result has been found
            Assertions.assertTrue(rs.next());
            logger.info("Similarity Search succeed {}", rs.getObject("product_vector"));

        } catch (java.sql.SQLException e) {
            System.out.println("[KO] " + e.getMessage());
        }
    }

    /**
     * Create table with JDBC
     *
     * @throws SQLException
     *      error
     */
    private static void createVectorTable() throws SQLException {
        // Create Connection
        try (Connection conn = jdbcDataSource.getConnection()) {

            // Create a Table with Embeddings
            conn.createStatement().execute("" +
                    "CREATE TABLE IF NOT EXISTS pet_supply_vectors (" +
                    "    product_id     TEXT PRIMARY KEY," +
                    "    product_name   TEXT," +
                    "    product_vector vector<float, 14>)");
            logger.info("Table created.");

            // Create a Search Index
            conn.createStatement().execute("" +
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector " +
                    "ON pet_supply_vectors(product_vector) " +
                    "USING 'StorageAttachedIndex'");
            logger.info("Index Created.");

            // Insert rows
            conn.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf1843','HealthyFresh - Chicken raw dog food',[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
            conn.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf1844','HealthyFresh - Beef raw dog food',[1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0])");
            conn.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pt0021','Dog Tennis Ball Toy',[0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0])");
            conn.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pt0041','Dog Ring Chew Toy',[0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0])");
            conn.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf7043','PupperSausage Bacon dog Treats',[0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1])");
            conn.createStatement().execute("" +
                    "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                    "VALUES ('pf7044','PupperSausage Beef dog Treats',[0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0])");
            logger.info("Table populated.");
        }
    }
}
