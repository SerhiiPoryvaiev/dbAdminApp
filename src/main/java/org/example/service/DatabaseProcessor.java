package org.example.service;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Abstract class that defines common operations for interacting with databases.
 * This class is extended by database-specific classes to implement operations for different database types.
 * @version 1.0
 * @author Serhii Poryvaiev
 */
public abstract class DatabaseProcessor {

    /** Database connection */
    protected Connection connection;

    /**
     * Constructor that initializes the database connection.
     *
     * @param connection the connection to the database
     */
    public DatabaseProcessor(Connection connection) {
        this.connection = connection;
    }

    /**
     * Retrieves a list of tables from the database and saves it to a file.
     * This method must be implemented by subclasses to handle database-specific logic.
     *
     * @param fileName the name of the file to save the table list
     * @param caseFormat the format for file names (uppercase/lowercase)
     */
    public abstract void getTableList(String fileName, String caseFormat);

    /**
     * Retrieves the schema of the database and saves it to a file.
     * This method must be implemented by subclasses to handle database-specific logic.
     *
     * @param fileName the name of the file to save the database schema
     */
    public abstract void getDatabaseSchema(String fileName);

    /**
     * Retrieves the schema of a specific table and saves it to a file.
     * This method must be implemented by subclasses to handle database-specific logic.
     *
     * @param tableName the name of the table
     * @param fileName the name of the file to save the table schema
     */
    public abstract void getTableSchema(String tableName, String fileName);

    /**
     * Retrieves data from a specific table and saves it to a file.
     * This method must be implemented by subclasses to handle database-specific logic.
     *
     * @param tableName the name of the table
     * @param fileName the name of the file to save the table data
     */
    public abstract void getTableData(String tableName, String fileName);

    /**
     * Converts the schema of the database to a new format.
     * This method must be implemented by subclasses to handle database-specific conversion logic.
     *
     * @param schemaName the name of the schema to convert
     * @return the converted schema as a string
     * @throws SQLException if an error occurs during the database interaction
     */
    public abstract String convertDatabaseSchema(String schemaName) throws SQLException;

    /**
     * Converts the schema of a specific table to a new format.
     * This method must be implemented by subclasses to handle database-specific conversion logic.
     *
     * @param tableName the name of the table to convert
     * @param fileName the name of the file to save the converted table schema
     * @throws SQLException if an error occurs during the database interaction
     */
    public abstract void convertTableSchema(String tableName, String fileName) throws SQLException;

    /**
     * Converts the data of a specific table to a new format.
     * This method must be implemented by subclasses to handle database-specific conversion logic.
     *
     * @param tableName the name of the table to convert
     * @param fileName the name of the file to save the converted table data
     */
    public abstract void convertTableData(String tableName, String fileName);
}
