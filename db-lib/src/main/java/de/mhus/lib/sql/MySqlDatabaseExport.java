/**
 * Copyright 2018 Mike Hummel
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.sql;
/**
 * Copyright Isocra Ltd 2004 You can use, modify and freely distribute this file as long as you
 * credit Isocra Ltd. There is no explicit or implied guarantee of functionality associated with
 * this file, use it at your own risk.
 *
 * <p>Changed by Mike Hummel 2012
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import de.mhus.lib.core.MCast;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.core.logging.Log;

/**
 * This class connects to a database and dumps all the tables and contents out to stdout in the form
 * of a set of SQL executable statements
 *
 * <p># Properties file for controlling db2sql.java
 *
 * <p># Driver information (make sure the appropriate classes/jars are on the classpath) #
 * ================== # # These are mandatory, you must provide appropriate values
 * driver.url=jdbc:mysql://localhost/testdb driver.class=com.mysql.jdbc.Driver
 *
 * <p># Information passed to DriverManger.getConnection #
 * ================================================ # # Put any information here that you want to
 * pass to the DriverManager, for example: user=<username> password=<password>
 *
 * <p># Configuration information # ========================= # # Optional information that you can
 * set to control which tables are output etc. # See target="_BLANK">DatabaseMetaData.getTables(...)
 * for information on how to use these. # Leave these blank to get all tables (note that only normal
 * tables are returned, not # views, system tables, temporary tables, synonyms or aliases) #
 * catalog= # schemaPattern= # tableName= # # You can also specify a quote character that is used to
 * surround column names. This is # useful if your tables contain any SQL-unfriendly characters such
 * as hyphens. This is # configurable so that you can set it up for your target database rather than
 * the source # database. Note however that if you do specify a quote character then the case of the
 * # table will probably become significant and this may not be what you want. Note also # that this
 * is not the same as the quote character for data values. For that a single # quote is used always.
 * Leave this blank for no quote character. # columnName.quoteChar="
 *
 * <p># dropTables = true
 */
public class MySqlDatabaseExport {

    /**
     * Dump the whole database to an SQL string
     *
     * @param props
     * @param writer
     * @return x
     */
    public static boolean dumpDB(IConfig props, PrintWriter writer) {
        Properties prop = new Properties();
        for (String key : props.getNodeKeys()) prop.setProperty(key, props.getExtracted(key));
        return dumpDB(prop, writer);
    }

    /**
     * Dump the whole database to an SQL string
     *
     * @param props
     * @param writer
     * @return x
     */
    public static boolean dumpDB(Properties props, PrintWriter writer) {
        String driverClassName = props.getProperty("driver.class");
        String driverURL = props.getProperty("driver.url");
        // Default to not having a quote character
        Connection dbConn = null;
        try {
            Class.forName(driverClassName);
            dbConn = DriverManager.getConnection(driverURL, props);
        } catch (Exception e) {
            Log.getLog(MySqlDatabaseExport.class).e("Unable to connect to database", e);
            return false;
        }
        boolean ret = dumpDB(dbConn, props, writer);
        try {
            dbConn.close();
        } catch (SQLException e) {
            Log.getLog(MySqlDatabaseExport.class).e("Unable to close database", e);
        }
        return ret;
    }

    public static boolean dumpDB(Connection dbConn, Properties props, PrintWriter writer) {
        try {
            // StringBuilder result = new StringBuilder();
            String catalog = props.getProperty("catalog");
            String schema = props.getProperty("schemaPattern");
            String tables = props.getProperty("tableName");
            String columnNameQuote = props.getProperty("columnName.quoteChar", "");
            boolean dropTables = props.getProperty("dropTables", "true").equals("true");

            DatabaseMetaData dbMetaData = dbConn.getMetaData();
            ResultSet rs = dbMetaData.getTables(catalog, schema, tables, null);
            if (!rs.next()) {
                Log.getLog(MySqlDatabaseExport.class)
                        .e(
                                "Unable to find any tables matching: catalog="
                                        + catalog
                                        + " schema="
                                        + schema
                                        + " tables="
                                        + tables);
                rs.close();
            } else {
                // Right, we have some tables, so we can go to work.
                // the details we have are
                // TABLE_CAT String => table catalog (may be null)
                // TABLE_SCHEM String => table schema (may be null)
                // TABLE_NAME String => table name
                // TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM
                // TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
                // REMARKS String => explanatory comment on the table
                // TYPE_CAT String => the types catalog (may be null)
                // TYPE_SCHEM String => the types schema (may be null)
                // TYPE_NAME String => type name (may be null)
                // SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of
                // a typed table (may be null)
                // REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are
                // created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
                // We will ignore the schema and stuff, because people might want to import it
                // somewhere else
                // We will also ignore any tables that aren't of type TABLE for now.
                // We use a do-while because we've already caled rs.next to see if there are any
                // rows
                do {
                    String tableName = rs.getString("TABLE_NAME");
                    String tableType = rs.getString("TABLE_TYPE");
                    if ("TABLE".equalsIgnoreCase(tableType)) {
                        writer.print("\n\n-- " + tableName);
                        if (dropTables) {
                            writer.print("\nDROP TABLE " + tableName + ";\n");
                        }
                        writer.print("\nCREATE TABLE " + tableName + " (\n");
                        ResultSet tableMetaData = dbMetaData.getColumns(null, null, tableName, "%");
                        boolean firstLine = true;
                        while (tableMetaData.next()) {
                            if (firstLine) {
                                firstLine = false;
                            } else {
                                // If we're not the first line, then finish the previous line with a
                                // comma
                                writer.print(",\n");
                            }
                            String columnName = tableMetaData.getString("COLUMN_NAME");
                            String columnType = tableMetaData.getString("TYPE_NAME");
                            // WARNING: this may give daft answers for some types on some databases
                            // (eg JDBC-ODBC link)
                            int columnSize = tableMetaData.getInt("COLUMN_SIZE");
                            String nullable = tableMetaData.getString("IS_NULLABLE");
                            String nullString = "NULL";
                            if ("NO".equalsIgnoreCase(nullable)) {
                                nullString = "NOT NULL";
                            }

                            boolean needSize = columnType.startsWith("VAR");

                            writer.print(
                                    "    "
                                            + columnNameQuote
                                            + columnName
                                            + columnNameQuote
                                            + " "
                                            + columnType);
                            if (needSize) writer.print(" (" + columnSize + ")");
                            writer.print(" " + nullString);
                        }
                        tableMetaData.close();

                        // Now we need to put the primary key constraint
                        try {
                            ResultSet primaryKeys =
                                    dbMetaData.getPrimaryKeys(catalog, schema, tableName);
                            // What we might get:
                            // TABLE_CAT String => table catalog (may be null)
                            // TABLE_SCHEM String => table schema (may be null)
                            // TABLE_NAME String => table name
                            // COLUMN_NAME String => column name
                            // KEY_SEQ short => sequence number within primary key
                            // PK_NAME String => primary key name (may be null)
                            String primaryKeyName = null;
                            StringBuilder primaryKeyColumns = new StringBuilder();
                            while (primaryKeys.next()) {
                                String thisKeyName = primaryKeys.getString("PK_NAME");
                                if ((thisKeyName != null && primaryKeyName == null)
                                        || (thisKeyName == null && primaryKeyName != null)
                                        || (thisKeyName != null
                                                && !thisKeyName.equals(primaryKeyName))
                                        || (primaryKeyName != null
                                                && !primaryKeyName.equals(thisKeyName))) {
                                    // the keynames aren't the same, so output all that we have so
                                    // far (if anything)
                                    // and start a new primary key entry
                                    if (primaryKeyColumns.length() > 0) {
                                        // There's something to output
                                        writer.print(",\n    PRIMARY KEY ");
                                        if (primaryKeyName != null
                                                && !primaryKeyName.equals("PRIMARY")) {
                                            writer.print(primaryKeyName);
                                        }
                                        writer.print("(" + primaryKeyColumns.toString() + ")");
                                    }
                                    // Start again with the new name
                                    primaryKeyColumns = new StringBuilder();
                                    primaryKeyName = thisKeyName;
                                }
                                // Now append the column
                                if (primaryKeyColumns.length() > 0) {
                                    primaryKeyColumns.append(", ");
                                }
                                primaryKeyColumns.append(primaryKeys.getString("COLUMN_NAME"));
                            }
                            if (primaryKeyColumns.length() > 0) {
                                // There's something to output
                                writer.print(",\n    PRIMARY KEY ");
                                if (primaryKeyName != null && !primaryKeyName.equals("PRIMARY")) {
                                    writer.print(primaryKeyName);
                                }
                                writer.print(" (" + primaryKeyColumns.toString() + ")");
                            }
                        } catch (SQLException e) {
                            // NB you will get this exception with the JDBC-ODBC link because it
                            // says
                            // [Microsoft][ODBC Driver Manager] Driver does not support this
                            // function
                            Log.getLog(MySqlDatabaseExport.class)
                                    .e(
                                            "Unable to get primary keys for table "
                                                    + tableName
                                                    + " because ",
                                            e);
                        }

                        writer.print("\n);\n");

                        // Right, we have a table, so we can go and dump it
                        dumpTable(dbConn, writer, tableName);
                    }
                } while (rs.next());
                rs.close();
            }
            return true;
        } catch (SQLException e) {
            Log.getLog(MySqlDatabaseExport.class)
                    .e(e); // To change body of catch statement use Options | File Templates.
        }
        return false;
    }

    /** dump this particular table to the string buffer */
    private static void dumpTable(Connection dbConn, PrintWriter result, String tableName) {
        try {
            // First we output the create table stuff
            PreparedStatement stmt = dbConn.prepareStatement("SELECT * FROM " + tableName);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Now we can output the actual data
            result.print("\n\n-- Data for " + tableName + "\n");
            int rowCnt = 0;
            while (rs.next()) {

                if (rowCnt % 50 != 0 && rowCnt != 0) result.print(",\n");
                else {
                    if (rowCnt != 0) {
                        result.print(";\n");
                    }
                    result.print("INSERT INTO " + tableName + " (");
                    for (int i = 0; i < columnCount; i++) {
                        if (i > 0) {
                            result.print(", ");
                        }
                        result.print(metaData.getColumnName(i + 1));
                    }
                    result.print(") VALUES ");
                }
                rowCnt++;
                result.print(" (");

                for (int i = 0; i < columnCount; i++) {
                    if (i > 0) {
                        result.print(", ");
                    }
                    Object value = rs.getObject(i + 1);
                    if (value == null) {
                        result.print("NULL");
                    } else if (value instanceof byte[]) {
                        //                    	InputStream stream = ((Blob)value).getBinaryStream();
                        //                    	result.print("'");
                        //                    	try {
                        //	                    	while(true) {
                        //	                    		int b = stream.read();
                        //	                    		if (b < 0) break;
                        //	                    		result.print(MCast.toHex2String(b));
                        //	                    	}
                        //	                    	stream.close();
                        //                    	} catch (IOException ioe) {}
                        //                        result.print("'");
                        byte[] bytes = (byte[]) value;
                        result.print("X'");
                        for (byte b : bytes) {
                            result.print(MCast.toHex2String(b));
                        }
                        result.print("'");
                    } else {
                        String outputValue = value.toString();
                        if (outputValue.indexOf('\\') > -1)
                            outputValue = outputValue.replaceAll("\\\\", "\\\\\\\\");
                        if (outputValue.indexOf('\'') > -1)
                            outputValue = outputValue.replaceAll("'", "\\\\'");
                        if (outputValue.indexOf('"') > -1)
                            outputValue = outputValue.replaceAll("\"", "\\\\\"");
                        result.print("'" + outputValue + "'");
                    }
                }
                result.print(")");
            }
            if (rowCnt != 0) result.print(";\n");
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            Log.getLog(MySqlDatabaseExport.class)
                    .e("Unable to dump table " + tableName + " because", e);
        }
    }

    /**
     * Main method takes arguments for connection to JDBC etc.
     *
     * @param args
     */
    public static void main(String[] args) {

        if (args.length != 1) {
            System.err.println("usage: db2sql <property file>");
        }
        // Right so there's one argument, we assume it's a property file
        // so lets open it
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(args[0]));
            PrintWriter pw = new PrintWriter(System.out);
            dumpDB(props, pw);
            pw.flush();
        } catch (IOException e) {
            Log.getLog("main").e("Unable to open property file", args[0], e);
        }
    }
}
