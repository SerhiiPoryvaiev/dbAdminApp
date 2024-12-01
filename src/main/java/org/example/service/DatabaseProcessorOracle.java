package org.example.service;

import java.io.*;
import java.sql.*;

/**
 * Implementation of {@link DatabaseProcessor} for Oracle database operations.
 * Class for working with Oracle-databases, extending the base DatabaseProcessor class.
 * @author Serhii Poryvaiev
 */
public class DatabaseProcessorOracle extends DatabaseProcessor {

    /**
     * Constructor initializing the connection to the database.
     *
     * @param connection a {@link Connection} object representing the connection to the Oracle database.
     */
    public DatabaseProcessorOracle(Connection connection) {
        super(connection);
    }

    /**
     * Exports the list of tables from the database to the specified file.
     *
     * @param fileName   the name of the file to save the table list. If {@code null}, "tables.txt" will be used.
     * @param caseFormat the case format for table names: {@code "uppercase"} for uppercase, {@code "lowercase"} for lowercase,
     *                   or {@code null} to keep the original case.
     */
    @Override
    public void getTableList(String fileName, String caseFormat) {
        String tableListQuery = "SELECT table_name FROM user_tables";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(tableListQuery);
             FileWriter writer = new FileWriter(fileName != null ? fileName : "tables.txt")) {

            System.out.println("Generating table list...");

            while (rs.next()) {
                String tableName = rs.getString(1);
                if ("uppercase".equals(caseFormat)) {
                    tableName = tableName.toUpperCase();
                } else if ("lowercase".equals(caseFormat)) {
                    tableName = tableName.toLowerCase();
                }
                writer.write(tableName + "\n");
            }

            System.out.println("Table list successfully saved to " + (fileName != null ? fileName : "tables.txt") + ".");

        } catch (SQLException | IOException e) {
            System.out.println("Error exporting table list: " + e.getMessage());
        }
    }

    /**
     * Exports the entire database schema to the specified file.
     *
     * @param fileName the name of the file to save the database schema. If {@code null}, "dbschema.sql" will be used.
     */
    @Override
    public void getDatabaseSchema(String fileName) {
        String schemaQuery = "SELECT table_name FROM user_tables";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(schemaQuery);
             FileWriter writer = new FileWriter(fileName != null ? fileName : "dbschema.sql")) {

            System.out.println("Generating database schema...");

            while (rs.next()) {
                String tableName = rs.getString(1);
                writer.write("-- Schema for table " + tableName + "\n");
                writer.write("CREATE TABLE " + tableName + " (\n");

                String columnQuery = "SELECT column_name, data_type, data_length, data_precision, data_scale, nullable "
                        + "FROM user_tab_columns WHERE table_name = '" + tableName + "'";

                try (Statement columnStmt = connection.createStatement();
                     ResultSet columnRs = columnStmt.executeQuery(columnQuery)) {

                    boolean first = true;
                    while (columnRs.next()) {
                        if (!first) {
                            writer.write(",\n");
                        }
                        first = false;

                        String columnName = columnRs.getString("column_name");
                        String dataType = columnRs.getString("data_type");
                        int dataLength = columnRs.getInt("data_length");
                        int dataPrecision = columnRs.getInt("data_precision");
                        int dataScale = columnRs.getInt("data_scale");
                        String isNullable = columnRs.getString("nullable");

                        writer.write("    " + columnName + " " + dataType);

                        if ("NUMBER".equalsIgnoreCase(dataType)) {
                            if (dataPrecision > 0) {
                                writer.write("(" + dataPrecision);
                                if (dataScale > 0) {
                                    writer.write("," + dataScale);
                                }
                                writer.write(")");
                            }
                        } else if (dataLength > 0 && !"NUMBER".equalsIgnoreCase(dataType)) {
                            writer.write("(" + dataLength + ")");
                        }

                        if ("N".equals(isNullable)) {
                            writer.write(" NOT NULL");
                        }
                    }

                    writer.write("\n);\n\n");
                }
            }

            System.out.println("Database schema successfully saved to " + (fileName != null ? fileName : "dbschema.sql") + ".");

        } catch (SQLException | IOException e) {
            System.out.println("Error exporting database schema: " + e.getMessage());
        }
    }

    /**
     * Exports the schema of a specific table to the specified file.
     *
     * @param tableName the name of the table whose schema will be exported.
     * @param fileName  the name of the file to save the table schema. If {@code null}, "table_schema_oracle.sql" will be used.
     */
    @Override
    public void getTableSchema(String tableName, String fileName) {
        String tableSchemaQuery = "SELECT column_name, data_type, data_length, data_default, nullable " +
                "FROM all_tab_columns WHERE table_name = '" + tableName + "'";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(tableSchemaQuery);
             FileWriter writer = new FileWriter(fileName != null ? fileName : "table_schema_oracle.sql")) {

            System.out.println("Generating schema for table " + tableName + "...");

            writer.write("-- Schema for table " + tableName + "\n");
            writer.write("CREATE TABLE " + tableName + " (\n");

            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    writer.write(",\n");
                }
                first = false;

                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                int dataLength = rs.getInt("data_length");
                String columnDefault = rs.getString("data_default");
                String isNullable = rs.getString("nullable");

                writer.write("    " + columnName + " " + dataType);
                if (dataLength > 0) {
                    writer.write("(" + dataLength + ")");
                }

                if (columnDefault != null) {
                    writer.write(" DEFAULT " + columnDefault);
                }

                if ("N".equals(isNullable)) {
                    writer.write(" NOT NULL");
                }
            }

            writer.write("\n);\n\n");

            System.out.println("Schema for table " + tableName + " successfully saved to " + (fileName != null ? fileName : "table_schema.sql") + ".");

        } catch (SQLException | IOException e) {
            System.out.println("Error exporting schema for table " + tableName + ": " + e.getMessage());
        }
    }

    /**
     * Exports the data of a specific table to the specified file.
     *
     * @param tableName the name of the table whose data will be exported.
     * @param fileName  the name of the file to save the table data.
     If {@code null}, the file will be named as {@code <table_name>_data.sql}.
     */
    @Override
    public void getTableData(String tableName, String fileName) {
        String query = "SELECT * FROM " + tableName;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             FileWriter writer = new FileWriter(fileName != null ? fileName : (tableName + "_data.sql"))) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (!rs.isBeforeFirst()) {
                System.out.println("No data found in table " + tableName);
                return;
            }

            System.out.println("Generating data for table " + tableName + "...");

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " (");

                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        sb.append(", ");
                    }
                    sb.append(metaData.getColumnName(i));
                }

                sb.append(") VALUES (");

                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        sb.append(", ");
                    }
                    Object value = rs.getObject(i);
                    if (value == null) {
                        sb.append("NULL");
                    } else if (value instanceof String) {
                        sb.append("'").append(value.toString().replace("'", "''")).append("'");
                    } else if (value instanceof Date) {
                        sb.append("TO_DATE('").append(new java.sql.Date(((Date) value).getTime()).toString()).append("', 'YYYY-MM-DD')");
                    } else if (value instanceof Timestamp) {
                        sb.append("TO_TIMESTAMP('").append(new java.sql.Timestamp(((Timestamp) value).getTime()).toString()).append("', 'YYYY-MM-DD HH24:MI:SS')");
                    } else {
                        sb.append(value.toString());
                    }
                }

                sb.append(");\n");
                writer.write(sb.toString());
            }
            System.out.println("Table data successfully saved to " + (fileName != null ? fileName : (tableName + "_data.sql")) + ".");

        } catch (SQLException | IOException e) {
            System.out.println("Error exporting table data: " + e.getMessage());
        }
    }


    /**
     * Extends functionality for converting Oracle database schema and data to MySQL format.
     */
    @Override
    public String convertDatabaseSchema(String schemaName) throws SQLException {
        StringBuilder schema = new StringBuilder();
        schema.append("-- Converted database schema from Oracle to MySQL\n");

        String query = "SELECT table_name FROM all_tables WHERE owner = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, schemaName.toUpperCase());
            ResultSet resultSet = preparedStatement.executeQuery();

            if (!resultSet.isBeforeFirst()) {
                System.out.println("No tables found in schema: " + schemaName);
                return schema.toString();
            }

            while (resultSet.next()) {
                String tableName = resultSet.getString("table_name");
                schema.append(getTableSchemaFromOracle(tableName)).append("\n");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving database schema: " + e.getMessage());
            throw e;
        }
        return schema.toString();
    }

    /**
     * Converts the schema of a specific Oracle table to MySQL format and writes it to a file.
     *
     * @param tableName the name of the table whose schema will be converted.
     * @param fileName  the file where the converted schema will be saved.
     * @throws SQLException if an SQL error occurs during the conversion.
     */
    @Override
    public void convertTableSchema(String tableName, String fileName) throws SQLException {
        StringBuilder schema = new StringBuilder();
        schema.append("-- Converted table schema from Oracle to MySQL\n");
        schema.append(getTableSchemaFromOracle(tableName));

        try (PrintWriter writer = new PrintWriter(fileName)) {
            writer.println(schema.toString());
            System.out.println("Schema for table " + tableName + " has been saved to " + fileName);
        } catch (FileNotFoundException e) {
            System.out.println("Error writing to file " + fileName + ": " + e.getMessage());
        }
    }

    /**
     * Converts the data of a specific Oracle table to MySQL format and writes it to a file.
     *
     * @param tableName the name of the table whose data will be converted.
     * @param fileName  the file where the converted data will be saved.
     */
    @Override
    public void convertTableData(String tableName, String fileName) {
        String query = "SELECT * FROM " + tableName;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            writer.write("-- Converted data from Oracle table " + tableName + " to MySQL table " + fileName + "\n");

            while (rs.next()) {
                StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);

                    if (value == null) {
                        sql.append("NULL");
                    } else if (value instanceof String) {
                        sql.append("'").append(value.toString().replace("'", "''")).append("'");
                    } else if (value instanceof Timestamp || value instanceof Date) {
                        sql.append("'").append(value.toString()).append("'");
                    } else {
                        sql.append(value);
                    }

                    if (i < columnCount) {
                        sql.append(", ");
                    }
                }
                sql.append(");\n");
                writer.write(sql.toString());
            }
            System.out.println("Data successfully converted from Oracle table " + tableName + " to MySQL format and saved to " + fileName);

        } catch (SQLException | IOException e) {
            System.out.println("Error during data conversion: " + e.getMessage());
        }
    }

    /**
     * Converts Oracle data types to MySQL data types.
     *
     * @param oracleType   the Oracle data type.
     * @param dataLength   the length of the Oracle data type.
     * @param dataPrecision the precision of the Oracle numeric type.
     * @param dataScale    the scale of the Oracle numeric type.
     * @return the equivalent MySQL data type.
     */
    private String convertOracleTypeToMySQL(String oracleType, int dataLength, Integer dataPrecision, Integer dataScale) {
        switch (oracleType) {
            case "VARCHAR2":
                return "VARCHAR(" + dataLength + ")";
            case "NUMBER":
                if (dataPrecision != null && dataPrecision > 0) {
                    if (dataScale != null && dataScale >= 0) {
                        return "DECIMAL(" + dataPrecision + ", " + dataScale + ")";
                    } else {
                        return "DECIMAL(" + dataPrecision + ", 0)";
                    }
                } else {
                    return "INT";
                }
            case "TIMESTAMP":
                return "DATETIME";
            case "CHAR":
                return "CHAR(" + dataLength + ")";
            case "CLOB":
                return "TEXT";
            case "DATE":
                return "DATE";
            default:
                return oracleType;
        }
    }

    /**
     * Retrieves the schema of an Oracle table and converts it to MySQL format.
     *
     * @param tableName the name of the Oracle table.
     * @return a string containing the MySQL-compatible schema of the table.
     * @throws SQLException if an SQL error occurs during the retrieval.
     */
    public String getTableSchemaFromOracle(String tableName) throws SQLException {
        StringBuilder schema = new StringBuilder();
        schema.append("CREATE TABLE ").append(tableName).append(" (\n");

        String query = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE " +
                "FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, tableName.toUpperCase());
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String dataType = resultSet.getString("DATA_TYPE");
                int dataLength = resultSet.getInt("DATA_LENGTH");
                Integer dataPrecision = resultSet.getInt("DATA_PRECISION");
                Integer dataScale = resultSet.getInt("DATA_SCALE");
                String nullable = resultSet.getString("NULLABLE");

                String mysqlType = convertOracleTypeToMySQL(dataType, dataLength, dataPrecision, dataScale);
                schema.append("    ").append(columnName).append(" ").append(mysqlType);
                if ("N".equals(nullable)) {
                    schema.append(" NOT NULL");
                }
                schema.append(",\n");
            }

            if (schema.length() > 0) {
                schema.setLength(schema.length() - 2);
            }
            schema.append("\n);\n");
        } catch (SQLException e) {
            System.out.println("Error retrieving schema for table " + tableName + ": " + e.getMessage());
            throw e;
        }
        return schema.toString();
    }
}
