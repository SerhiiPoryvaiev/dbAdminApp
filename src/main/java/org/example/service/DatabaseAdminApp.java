package org.example.service;

import org.apache.commons.cli.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiConsumer;

/**
 * Main class for DatabaseAdminApp, providing CLI-based interaction
 * for database operations such as schema conversion, data export, and more.
 *
 * @version 1.0
 * @author Serhii Poryvaiev
 */
public class DatabaseAdminApp {

    /** A map containing command handlers for various commands. */
    private static final Map<String, BiConsumer<DatabaseProcessor, CommandLine>> commandHandlers = new HashMap<>();

    /**
     * Main entry point for the application. Sets up command line options,
     * processes user input, and executes commands.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        Options options = setupCommandLineOptions();
        CommandLineParser parser = new DefaultParser();
        Scanner scanner = new Scanner(System.in);
        setupCommandHandlers();

        while (true) {
            try {
                String input = scanner.nextLine();
                if ("exit".equalsIgnoreCase(input.trim())) {
                    System.out.println("Exiting the program.");
                    break;
                }

                String[] argsArray = input.split(" ");
                CommandLine cmd = parser.parse(options, argsArray);

                String url = cmd.getOptionValue("url");
                String username = cmd.getOptionValue("username");
                String password = cmd.getOptionValue("password");

                try (Connection conn = connectToDatabase(url, username, password)) {
                    if (conn == null) continue;

                    String dbType = cmd.getOptionValue("dbtype").toLowerCase();
                    DatabaseProcessor databaseProcessor = createDatabaseProcessor(conn, dbType);
                    if (databaseProcessor == null) {
                        System.out.println("Unsupported database type: " + dbType);
                        continue;
                    }

                    handleCommand(databaseProcessor, cmd);
                } catch (SQLException e) {
                    System.out.println("Database connection error: " + e.getMessage());
                }
            } catch (ParseException e) {
                System.out.println("Parsing error: " + e.getMessage());
            }
        }
        scanner.close();
    }

    /**
     * Configures the command-line options used by the application.
     *
     * @return Options object containing all CLI options
     */
    private static Options setupCommandLineOptions() {
        Options options = new Options();
        options.addOption("url", true, "Database URL");
        options.addOption("username", true, "Username");
        options.addOption("password", true, "Password");
        options.addOption("command", true, "Command to execute");
        options.addOption("file", true, "File name (optional)");
        options.addOption("case", true, "File name format (uppercase/lowercase)");
        options.addOption("tablename", true, "Specific table name");
        options.addOption("dbtype", true, "Database type (oracle/mysql)");
        options.addOption("schema", true, "Schema name for conversion");
        return options;
    }

    /**
     * Establishes a connection to the database.
     *
     * @param url      the database URL
     * @param username the database username
     * @param password the database password
     * @return Connection to the database, or null if the connection fails
     */
    private static Connection connectToDatabase(String url, String username, String password) {
        try {
            Connection conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connected to the database.");
            return conn;
        } catch (SQLException e) {
            System.out.println("Connection failed: " + e.getMessage());
            return null;
        }
    }

    /**
     * Creates a DatabaseProcessor instance based on the specified database type.
     *
     * @param connection the active database connection
     * @param dbType     the type of database (e.g., "mysql", "oracle")
     * @return a DatabaseProcessor instance, or null if the type is unsupported
     */
    private static DatabaseProcessor createDatabaseProcessor(Connection connection, String dbType) {
        switch (dbType) {
            case "mysql":
                return new DatabaseProcessorMySQL(connection);
            case "oracle":
                return new DatabaseProcessorOracle(connection);
            default:
                return null;
        }
    }

    /**
     * Sets up command handlers for various commands supported by the application.
     */
    private static void setupCommandHandlers() {
        commandHandlers.put("tablelist", (processor, cmd) ->
                processor.getTableList(cmd.getOptionValue("file"), cmd.getOptionValue("case"))
        );
        commandHandlers.put("dbschema", (processor, cmd) ->
                processor.getDatabaseSchema(cmd.getOptionValue("file"))
        );
        commandHandlers.put("tableschema", (processor, cmd) -> {
            String tableName = cmd.getOptionValue("tablename");
            if (tableName != null) processor.getTableSchema(tableName, cmd.getOptionValue("file"));
            else System.out.println("Table name is required for schema export.");
        });
        commandHandlers.put("tabledata", (processor, cmd) -> {
            String tableName = cmd.getOptionValue("tablename");
            if (tableName != null) processor.getTableData(tableName, cmd.getOptionValue("file"));
            else System.out.println("Table name is required for data export.");
        });
        commandHandlers.put("convertdbschema", (processor, cmd) -> {
            String schemaName = cmd.getOptionValue("schema");
            if (schemaName != null) {
                try {
                    saveToFile(cmd.getOptionValue("file"), processor.convertDatabaseSchema(schemaName));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            else System.out.println("Schema name is required for conversion.");
        });
        commandHandlers.put("converttable", (processor, cmd) -> {
            String tableName = cmd.getOptionValue("tablename");
            if (tableName != null) {
                try {
                    processor.convertTableSchema(tableName, cmd.getOptionValue("file"));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            else System.out.println("Table name is required for table conversion.");
        });
        commandHandlers.put("convertdata", (processor, cmd) -> {
            String tableName = cmd.getOptionValue("tablename");
            if (tableName != null) processor.convertTableData(tableName, cmd.getOptionValue("file"));
            else System.out.println("Table name is required for data conversion.");
        });
    }

    /**
     * Executes the specified command by invoking the corresponding handler.
     *
     * @param processor the DatabaseProcessor to use for database operations
     * @param cmd       the CommandLine object containing parsed user input
     */
    private static void handleCommand(DatabaseProcessor processor, CommandLine cmd) {
        String command = cmd.getOptionValue("command").toLowerCase();
        BiConsumer<DatabaseProcessor, CommandLine> handler = commandHandlers.get(command);
        if (handler != null) {
            handler.accept(processor, cmd);
        } else {
            System.out.println("Unsupported command: " + command);
        }
    }

    /**
     * Saves the provided content to a file.
     *
     * @param fileName the name of the file to save the content to
     * @param content  the content to save
     */
    private static void saveToFile(String fileName, String content) {
        if (fileName != null) {
            try (PrintWriter writer = new PrintWriter(fileName)) {
                writer.println(content);
                System.out.println("File successfully saved: " + fileName);
            } catch (FileNotFoundException e) {
                System.out.println("Error saving to file: " + e.getMessage());
            }
        } else {
            System.out.println("File name is required to save the result.");
        }
    }
}
