/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;

import de.mhus.lib.adb.query.AQueryCreator;
import de.mhus.lib.annotations.adb.DbType;
import de.mhus.lib.core.MCast;
import de.mhus.lib.core.MDate;
import de.mhus.lib.core.MSql;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.core.parser.Parser;
import de.mhus.lib.core.parser.ParsingPart;
import de.mhus.lib.core.util.MObject;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.commonparser.Common2SqlCompiler;
import de.mhus.lib.sql.parser.FunctionPart;
import de.mhus.lib.sql.parser.ICompiler;
import de.mhus.lib.sql.parser.SqlCompiler;

/**
 * The dialect implements database vendor specific behaviors. The behavior outside this class should
 * be the same. This includes an abstraction of the database definition the query language and
 * execution behavior. The dialect also normalize the naming.
 *
 * <p>This class can compare a configuration with a database table structure and can modify the
 * database structure without deleting existing tables.
 *
 * <p>TODO: on request: remove other columns TODO: views, foreign keys
 *
 * @author mikehummel
 */
public abstract class Dialect extends MObject implements ICompiler, AQueryCreator {

    // POSSIBLE TYPES OF COLUMNS

    public static final String I_TYPE = "type";
    public static final String I_UNIQUE = "unique";

    public static final String I_NAME = "name";

    public static final String I_TABLE = "table";

    public static final String I_FIELDS = "fields";

    public static final String I_HINTS = "hints";

    public static final String K_PRIMARY_KEY = "primary_key";

    public static final String K_NAME = "name";

    public static final String K_TYPE = "type";

    public static final String K_SIZE = "size";

    public static final String K_DEFAULT = "default";

    public static final String K_NOT_NULL = "notnull";

    public static final String K_CATEGORIES = "category";

    public static final String K_DESCRIPTION = "description";

    public static final String K_HINTS = "hints";

    public static final String C_VIRTUAL = "[virtual]";

    public static final String C_PRIMARY_KEY = "[pk]";

    public static final String C_ENUMERATION = "[enum]";

    private Parser sqlParser = new SqlCompiler(this);
    private Parser commonParser = new Common2SqlCompiler(this);

    /**
     * Return the named type for a TYPE enum value. Use this function to be sure you have all hacks
     * included.
     *
     * @param type The type enum
     * @return x null if type is null/unknown or the name
     */
    public static String typeEnumToString(DbType.TYPE type) {
        if (type == null) return null;
        if (type == DbType.TYPE.UNKNOWN) return null;
        return type.name().toLowerCase();
    }

    /**
     * Create a database structure from configuration.
     *
     * @param data
     * @param db
     * @param caoMeta
     * @param cleanup
     * @throws Exception
     */
    public void createStructure(
            IConfig data, DbConnection db, MetadataBundle caoMeta, boolean cleanup)
            throws Exception {
        createTables(data, db, caoMeta, cleanup);
        createIndexes(data, db, caoMeta, cleanup);
        createData(data, db);
    }

    /**
     * Create or Update the defined tables. The config object need a bundle of 'table'
     * configurations which define the needed table structure. Example: [config] [table name='table
     * name' primary_key='field1,field2,...'] [field name='field name' prefix='prefix_' type='field
     * type' default='def value' notnull=yes/no /] [/table] [/config]
     *
     * @param data
     * @param db
     * @param caoBundle
     * @param cleanup
     * @throws Exception
     */
    public abstract void createTables(
            IConfig data, DbConnection db, MetadataBundle caoBundle, boolean cleanup)
            throws Exception;

    /**
     * Create or update indexes. The configuration need a bundle of 'index' elements to define the
     * indexes. Example: [config] [index name='name' table='table name' btree=yes/no unique=yes/no
     * fields='field1,field2,...'/] [/config]
     *
     * @param data
     * @param db
     * @param caoMeta
     * @param cleanup
     * @throws Exception
     */
    public abstract void createIndexes(
            IConfig data, DbConnection db, MetadataBundle caoMeta, boolean cleanup)
            throws Exception;

    /**
     * Execute 'data' configs: select = a select query to define a condition and/or data set set and
     * column = set a value in the config to the value from column condition = found,not
     * found,error,no error
     *
     * @param data
     * @param db
     * @throws Exception
     */
    public abstract void createData(IConfig data, DbConnection db) throws Exception;

    protected String getFieldConfig(IConfig f) {
        String type = getDbType(f);

        String ret = normalizeColumnName(f.getString("name", null)) + " " + type;

        String def = f.getExtracted("default");
        if (def != null) {
            def = getDbDef(def);
            ret = ret + " DEFAULT " + def;
        }
        boolean notNull = f.getBoolean("notnull", false);
        if (notNull) ret = ret + " NOT NULL";
        else ret = ret + " NULL";
        return ret;
    }

    /**
     * Returns a formated default value.
     *
     * @param def
     * @return x
     */
    protected String getDbDef(String def) {
        try {
            Double.valueOf(def);
        } catch (NumberFormatException e) {
            def = MSql.quoteSQL(def);
        }
        return def;
    }

    /**
     * Return a normalized cao type from the config.
     *
     * @param f
     * @return x
     */
    protected SqlMetaDefinition.TYPE getCaoType(IConfig f) {
        String type = f.getString("type", DbType.TYPE.STRING.name()).toUpperCase();
        SqlMetaDefinition.TYPE t = SqlMetaDefinition.TYPE.STRING;
        if (f.getString(K_CATEGORIES, "").indexOf(C_ENUMERATION) > -1) {
            t = SqlMetaDefinition.TYPE.STRING;
        } else if (type.equals(DbType.TYPE.STRING.name())
                || type.equals("CHAR")
                || type.equals("VARCHAR")) {
        } else if (type.equals(DbType.TYPE.INT.name()) || type.equals("INTEGER")) {
            t = SqlMetaDefinition.TYPE.LONG;
        } else if (type.equals(DbType.TYPE.BIGDECIMAL.name())) {
            t = SqlMetaDefinition.TYPE.STRING;
        } else if (type.equals("DATE")) {
            t = SqlMetaDefinition.TYPE.DATETIME;
        } else if (type.equals(DbType.TYPE.DATETIME.name())) {
            t = SqlMetaDefinition.TYPE.DATETIME;
        } else if (type.equals("TIME")) {
            t = SqlMetaDefinition.TYPE.DATETIME;
        } else if (type.equals("TIMESTAMP")) {
            t = SqlMetaDefinition.TYPE.DATETIME;
        } else if (type.equals(DbType.TYPE.BOOL.name()) || type.equals("BOOLEAN")) {
            t = SqlMetaDefinition.TYPE.BOOLEAN;
        } else if (type.equals(DbType.TYPE.BLOB.name())) {
            t = SqlMetaDefinition.TYPE.BINARY;
        } else if (type.equals(DbType.TYPE.DOUBLE.name())) {
            t = SqlMetaDefinition.TYPE.DOUBLE;
        } else if (type.equals(DbType.TYPE.FLOAT.name())) {
            t = SqlMetaDefinition.TYPE.DOUBLE;
        } else if (type.equals("TEXT")) {
        } else if (type.equals("LONGTEXT")) {
        } else if (type.equals("LONGBLOB")) {
            t = SqlMetaDefinition.TYPE.BINARY;
        } else if (type.equals(DbType.TYPE.UUID.name())) {
            t = SqlMetaDefinition.TYPE.ELEMENT;
            //			} else
            //			if (type.equals("NUMERIC")) {
            //				t = CaoMetaDefinition.TYPE.BIGDECIMAL;
        }
        return t;
    }

    /**
     * Return a database specific type for the normalized type from configuration.
     *
     * @param f
     * @return x
     */
    public String getDbType(IConfig f) {
        return getDbType(f.getString("type", "string"), f.getString("size", "100"));
    }

    /**
     * Return a database specific type for the normalized type from the type and size.
     *
     * @param type The general type name - see const
     * @param size the size, if needed
     * @return x
     */
    public String getDbType(String type, String size) {
        String t = type.toUpperCase();
        if (t.equals(DbType.TYPE.STRING.name()) || t.equals("CHAR") || t.equals("VARCHAR")) {
            t = "VARCHAR(" + size + ")";
        } else if (t.equals(DbType.TYPE.INT.name()) || t.equals("INTEGER")) {
            t = "INT";
        } else if (t.equals(DbType.TYPE.LONG.name())) {
            t = "BIGINT";
        } else if (t.equals("DATE")) {
            t = "DATE";
        } else if (t.equals(DbType.TYPE.DATETIME.name())) {
            t = "DATETIME";
        } else if (t.equals("TIME")) {
            t = "TIME";
        } else if (t.equals("TIMESTAMP")) {
            t = "TIMESTAMP";
        } else if (t.equals(DbType.TYPE.BOOL.name()) || t.equals("BOOLEAN")) {
            t = "TINYINT";
        } else if (t.equals(DbType.TYPE.BLOB.name())) {
            t = "BLOB";
        } else if (t.equals(DbType.TYPE.DOUBLE.name())) {
            t = "DOUBLE";
        } else if (t.equals(DbType.TYPE.FLOAT.name())) {
            t = "FLOAT";
        } else if (t.equals("TEXT")) {
            t = "TEXT";
        } else if (t.equals("LONGTEXT")) {
            t = "LONGTEXT";
        } else if (t.equals("LONGBLOB")) {
            t = "LONGBLOB";
        } else if (t.equals(DbType.TYPE.UUID.name())) {
            t = "VARCHAR(40)";
        } else if (t.equals(DbType.TYPE.BIGDECIMAL.name())) {
            t = "NUMERIC";
        }
        return t;
    }

    /**
     * Return a valid index name.
     *
     * @param tableName
     * @param tableOrg
     * @return x
     * @throws Exception
     */
    public abstract String normalizeIndexName(String tableName, String tableOrg) throws Exception;

    /**
     * Return a valid table name.
     *
     * @param tableName
     * @return x
     * @throws Exception
     */
    public abstract String normalizeTableName(String tableName) throws Exception;

    /**
     * Return a valid column name.
     *
     * @param columnName
     * @return x
     */
    public abstract String normalizeColumnName(String columnName);

    /**
     * Return a parser object to parse a sql query. The parser should change the output to be
     * specialized to the database type.
     *
     * @param language
     * @return x
     * @throws MException
     */
    public Parser getQueryParser(String language) throws MException {
        //		return new SimpleQueryParser(); // from dialect
        //		return new SqlCompiler(); // from dialect

        if (language == null || JdbcConnection.LANGUAGE_SQL.equals(language)) return sqlParser;

        if (DbConnection.LANGUAGE_COMMON.equals(language)) return commonParser;

        throw new MException(this, "language not supported", language);
    }

    /** Interface for the parser. */
    @Override
    public boolean isParseAttributes() {
        return true;
    }

    /** Interface for the parser. */
    @Override
    public ParsingPart compileFunction(FunctionPart function) {
        return function;
    }

    @Override
    public String toSqlDateValue(Object value) {
        if (value == null) return "null";
        if (value instanceof Calendar) return toSqlDate(((Calendar) value).getTime());
        if (value instanceof Date) return toSqlDate((Date) value);
        if (value instanceof LocalDateTime) 
            return toSqlDate(  MDate.toDate( (LocalDateTime) value, null));
        if (value instanceof LocalDate)
            return toSqlDate(  MDate.toDate( (LocalDate) value, null));
        if (value instanceof Number) {
            Date date = new Date(((Number) value).longValue());
            return toSqlDate(date);
        }
        Date date = MCast.toDate(value, null);
        if (date == null) return "null";
        return toSqlDate(date);
    }

    public String toSqlDate(Date date) {
        return "'" + MDate.toIsoDate(date) + "'";
    }

    @Override
    public String valueToString(Object value) {
        return MCast.objectToString(value);
    }

    /**
     * Detects the language of this query string. By default it will return null what means the
     * default language.
     *
     * <p>It will detect the common language.
     *
     * @param sql
     * @return x
     */
    public String detectLanguage(String sql) {
        if (sql == null) return null;
        if (sql.startsWith("<common>")) return DbConnection.LANGUAGE_COMMON;
        return null;
    }

    public void prepareConnection(Connection con) throws SQLException {
        con.setAutoCommit(false);
    }

    public static Dialect findDialect(String driver) {
        Dialect dialect = null;
        if (driver != null) {
            driver = driver.toLowerCase();
            if (driver.indexOf("hsql") > -1) dialect = new DialectHsqldb();
            else if (driver.indexOf("mysql") > -1 || driver.indexOf("mariadb") > -1)
                dialect = new DialectMysql();
            else if (driver.indexOf("postgresql") > -1) dialect = new DialectPostgresql();
            else if (driver.indexOf("h2 ") > -1) dialect = new DialectH2();
        }
        if (dialect == null) {
            dialect = new DialectDefault();
        }
        return dialect;
    }
}
