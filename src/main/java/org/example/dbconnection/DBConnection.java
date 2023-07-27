package org.example.dbconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public enum DBConnection {
    INSTANCE;

    private final String url = "jdbc:postgresql://localhost:5432/postgres";
    private final String username = "postgres";
    private final String password = "example";

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
}
