package org.example.service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;

/**
 * Implementation of {@link DatabaseProcessor} for MySQL database operations.
 * Class for working with MySQL-databases, extending the base DatabaseProcessor class.
 * @version 1.0
 * @author Serhii Poryvaiev
 */
public class DatabaseProcessorMySQL extends DatabaseProcessor {

    /**
     * Constructs a {@code DatabaseProcessorMySQL} instance with the provided database connection.
     *
     * @param connection the {@link Connection} to the MySQL database
     */
    public DatabaseProcessorMySQL(Connection connection) {
        super(connection);
    }

    /**
     * Retrieves the list of tables from the MySQL database and writes it to a file.
     *
     * @param fileName   the name of the file where the table list will be saved;
     *                   defaults to "tables.txt" if {@code null}
     * @param caseFormat specifies the case format for table names ("uppercase" or "lowercase");
     *                   or {@code null} to keep the original case.
     */
    @Override
    public void getTableList(String fileName, String caseFormat) {
        String tableListQuery = "SHOW TABLES";

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
     * Retrieves the schema of all tables in the MySQL database and writes it to a file.
     *
     * @param fileName the name of the file where the schema will be saved;
     *                 defaults to "dbschema.sql" if {@code null}
     */
    @Override
    public void getDatabaseSchema(String fileName) {
        String schemaQuery = "SHOW TABLES";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(schemaQuery);
             FileWriter writer = new FileWriter(fileName != null ? fileName : "dbschema.sql")) {

            System.out.println("Generating database schema...");

            while (rs.next()) {
                String tableName = rs.getString(1);
                writer.write("-- Schema for table " + tableName + "\n");
                writer.write("CREATE TABLE " + tableName + " (\n");

                String columnQuery = "SHOW COLUMNS FROM " + tableName;

                try (Statement columnStmt = connection.createStatement();
                     ResultSet columnRs = columnStmt.executeQuery(columnQuery)) {

                    boolean first = true;
                    while (columnRs.next()) {
                        if (!first) {
                            writer.write(",\n");
                        }
                        first = false;

                        String columnName = columnRs.getString("Field");
                        String dataType = columnRs.getString("Type");
                        String isNullable = columnRs.getString("Null");

                        writer.write("    " + columnName + " " + dataType);

                        if ("NO".equalsIgnoreCase(isNullable)) {
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
     * Retrieves the schema of a specific table in the MySQL database and writes it to a file.
     *
     * @param tableName the name of the table whose schema will be retrieved
     * @param fileName  the name of the file where the schema will be saved;
     *                  defaults to "table_schema_mySQL.sql" if {@code null}
     */
    @Override
    public void getTableSchema(String tableName, String fileName) {
        String tableSchemaQuery = "SHOW COLUMNS FROM " + tableName;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(tableSchemaQuery);
             FileWriter writer = new FileWriter(fileName != null ? fileName : "table_schema_mySQL.sql")) {

            System.out.println("Generating schema for table " + tableName + "...");

            writer.write("-- Schema for table " + tableName + "\n");
            writer.write("CREATE TABLE " + tableName + " (\n");

            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    writer.write(",\n");
                }
                first = false;

                String columnName = rs.getString("Field");
                String dataType = rs.getString("Type");
                String isNullable = rs.getString("Null");

                writer.write("    " + columnName + " " + dataType);

                if ("NO".equalsIgnoreCase(isNullable)) {
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
     * Retrieves the data from a specific table in the MySQL database and writes it to a file in SQL insert format.
     *
     * @param tableName the name of the table whose data will be retrieved
     * @param fileName  the name of the file where the data will be saved;
     *                  defaults to "[tableName]_data.sql" if {@code null}
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
     * Converts the schema of the entire MySQL database to Oracle-compatible SQL syntax.
     *
     * @param schemaName the name of the MySQL database schema to convert
     * @return a {@link String} containing the converted Oracle-compatible schema
     * @throws SQLException if a database access error occurs during schema retrieval
     */
    @Override
    public String convertDatabaseSchema(String schemaName) throws SQLException {
        StringBuilder schema = new StringBuilder();
        schema.append("-- Converted database schema from MySQL to Oracle\n");

        String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, schemaName);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String tableName = resultSet.getString("table_name");
                schema.append(getTableSchemaFromMySQL(tableName)).append("\n");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving database schema: " + e.getMessage());
            throw e;
        }
        return schema.toString();
    }

    /**
     * Converts the schema of a specific MySQL table to Oracle-compatible SQL syntax and saves it to a file.
     *
     * @param tableName the name of the table to convert
     * @param fileName  the name of the file where the converted schema will be saved
     * @throws SQLException if a database access error occurs during schema retrieval
     */
    @Override
    public void convertTableSchema(String tableName, String fileName) throws SQLException {
        StringBuilder schema = new StringBuilder();
        schema.append("CREATE TABLE ").append(tableName).append(" (\n");

        String query = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE " +
                "FROM information_schema.columns WHERE table_name = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, tableName);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String dataType = resultSet.getString("DATA_TYPE");
                String columnType = resultSet.getString("COLUMN_TYPE");
                int dataLength = resultSet.getInt("CHARACTER_MAXIMUM_LENGTH");
                String nullable = resultSet.getString("IS_NULLABLE");

                int precision = 0;
                int scale = 0;
                if (columnType.toUpperCase().startsWith("DECIMAL")) {
                    String[] parts = columnType.replaceAll("[^0-9,]", "").split(",");
                    if (parts.length > 0) {
                        precision = Integer.parseInt(parts[0]);
                    }
                    if (parts.length > 1) {
                        scale = Integer.parseInt(parts[1]);
                    }
                }

                String oracleType = convertMySQLTypeToOracle(dataType, dataLength, precision, scale);
                schema.append("    ").append(columnName).append(" ").append(oracleType);
                if ("NO".equals(nullable)) {
                    schema.append(" NOT NULL");
                }
                schema.append(",\n");
            }

            if (schema.length() > 0) {
                schema.setLength(schema.length() - 2); // Убираем лишнюю запятую
            }
            schema.append("\n);\n");
        } catch (SQLException e) {
            System.out.println("Error retrieving schema for table " + tableName + ": " + e.getMessage());
            throw e;
        }

        try (PrintWriter writer = new PrintWriter(fileName)) {
            writer.println(schema.toString());
            System.out.println("Schema for table " + tableName + " has been saved to " + fileName);
        } catch (Exception e) {
            System.out.println("Error writing to file " + fileName + ": " + e.getMessage());
        }
    }

    /**
     * Converts the data from a MySQL table to Oracle-compatible SQL insert statements and saves it to a file.
     *
     * @param tableName the name of the table whose data will be converted
     * @param fileName  the name of the file where the converted data will be saved
     */
    @Override
    public void convertTableData(String tableName, String fileName) {
        String query = "SELECT * FROM " + tableName;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {

            while (rs.next()) {
                StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
                int columnCount = rs.getMetaData().getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    if (value instanceof String) {
                        sql.append("'").append(value.toString().replace("'", "''")).append("'");
                    } else if (value instanceof Timestamp) {
                        sql.append("TO_TIMESTAMP('").append(value.toString()).append("', 'YYYY-MM-DD HH24:MI:SS.FF')");
                    } else if (value instanceof Date) {
                        sql.append("TO_DATE('").append(new java.text.SimpleDateFormat("yyyy-MM-dd").format(value)).append("', 'YYYY-MM-DD')");
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
            System.out.println("Data converted from MySQL to Oracle format in " + fileName);
        } catch (SQLException | IOException e) {
            System.out.println("Error converting data: " + e.getMessage());
        }
    }

    /**
     * Retrieves the schema of a specific MySQL table and converts it to Oracle-compatible SQL syntax.
     *
     * @param tableName the name of the table whose schema will be converted
     * @return a {@link String} containing the converted Oracle-compatible table schema
     * @throws SQLException if a database access error occurs during schema retrieval
     */
    public String getTableSchemaFromMySQL(String tableName) throws SQLException {
        StringBuilder schema = new StringBuilder();
        schema.append("CREATE TABLE ").append(tableName).append(" (\n");

        String query = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE " +
                "FROM information_schema.columns WHERE table_name = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, tableName);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String dataType = resultSet.getString("DATA_TYPE");
                String columnType = resultSet.getString("COLUMN_TYPE");
                int dataLength = resultSet.getInt("CHARACTER_MAXIMUM_LENGTH");
                String nullable = resultSet.getString("IS_NULLABLE");

                int precision = 0;
                int scale = 0;
                if (columnType.toUpperCase().startsWith("DECIMAL")) {
                    String[] parts = columnType.replaceAll("[^0-9,]", "").split(",");
                    if (parts.length > 0) {
                        precision = Integer.parseInt(parts[0]);
                    }
                    if (parts.length > 1) {
                        scale = Integer.parseInt(parts[1]);
                    }
                }

                String oracleType = convertMySQLTypeToOracle(dataType, dataLength, precision, scale);
                schema.append("    ").append(columnName).append(" ").append(oracleType);
                if ("NO".equals(nullable)) {
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

    /**
     * Converts a MySQL data type to the equivalent Oracle data type.
     *
     * @param mysqlType   the MySQL data type
     * @param dataLength  the maximum length of the data
     * @param precision   the precision for numeric types
     * @param scale       the scale for numeric types
     * @return a {@link String} representing the Oracle data type
     */
    private String convertMySQLTypeToOracle(String mysqlType, int dataLength, int precision, int scale) {
        switch (mysqlType.toUpperCase()) {
            case "VARCHAR":
                return "VARCHAR2(" + dataLength + ")";
            case "INT":
                return "NUMBER(10)";
            case "BIGINT":
                return "NUMBER(19)";
            case "TEXT":
                return "CLOB";
            case "DATETIME":
            case "TIMESTAMP":
                return "TIMESTAMP";
            case "DATE":
                return "DATE";
            case "CHAR":
                return "CHAR(" + dataLength + ")";
            case "BLOB":
                return "BLOB";
            case "DECIMAL":
                return "NUMBER(" + precision + ", " + scale + ")";
            default:
                return mysqlType;
        }
    }
}
