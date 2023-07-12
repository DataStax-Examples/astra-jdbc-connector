package com.datastax.astra.jdbc.exceptions;

import java.sql.SQLFeatureNotSupportedException;

public class AstraJdbcNotSupportedOperation extends SQLFeatureNotSupportedException {

    public AstraJdbcNotSupportedOperation(String reason) {
        super("Astra Jdbc Driver does not support this method '" + reason + "'");
    }
}
