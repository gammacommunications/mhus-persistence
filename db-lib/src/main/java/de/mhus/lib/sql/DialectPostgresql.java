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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import de.mhus.lib.adb.query.ALimit;
import de.mhus.lib.adb.query.APrint;
import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.annotations.adb.DbType;
import de.mhus.lib.core.MSql;
import de.mhus.lib.core.config.IConfig;

/**
 * This class can compare a configuration with a database table structure and can modify the
 * database structure without deleting existing tables.
 *
 * <p>TODO: on request: remove other columns TODO: views, foreign keys TODO: data !!!
 *
 * @author mikehummel
 */
public class DialectPostgresql extends DialectDefault {

    @Override
    public String normalizeColumnName(String columnName) {
        //		if ("key".equals(columnName))
        //			return "key_";
        //		return columnName;
        return columnName + "_"; // TODO not working at all
    }

    @Override
    public String normalizeIndexName(String tableName, String tableOrg) throws Exception {
        return (tableOrg + tableName).toLowerCase();
    }

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String toSqlDate(Date date) {
        synchronized (dateFormat) {
            return "'" + dateFormat.format(date) + "'";
        }
    }

    @Override
    public String toBoolValue(boolean value) {
        return value ? "'true'" : "'false'";
    }

    @Override
    protected void createTableLastCheck(IConfig ctable, String tn, StringBuilder sql) {}

    @Override
    public String escape(String text) {
        String ret = MSql.escape(text);
        if (ret == null || text == null) return ret;
        if (ret.indexOf('\\') < 0) return ret;
        return ret.replaceAll("\\\\", "\\\\\\\\");
    }

    @Override
    protected boolean equalTypes(String should, String is, int fSize) {
        is = is.toUpperCase();
        if (is.indexOf("CHAR") >= 0) {
            is = is + "(" + fSize + ")"; // add size to type
        } else if (is.equals("INT4")) is = "INT";
        else if (is.equals("FLOAT8")) is = "DOUBLE PRECISION";

        return should.equals(is);
    }

    @Override
    public String getDbType(String type, String size) {
        String t = type.toUpperCase();

        if (t.equals(DbType.TYPE.BLOB.name())) t = "BYTEA";
        else if (t.equals(DbType.TYPE.DATETIME.name())) t = "TIMESTAMP";
        else if (t.equals(DbType.TYPE.DOUBLE.name())) t = "DOUBLE PRECISION";
        else if (t.equals(DbType.TYPE.BOOL.name())) t = "BOOLEAN";
        else t = super.getDbType(type, size);

        return t;
    }

    @Override
    protected ResultSet findTable(DatabaseMetaData meta, String name) throws SQLException {
        return meta.getTables(null, null, name.toLowerCase(), new String[] {"TABLE"});
    }

    @Override
    protected ResultSet findColumn(DatabaseMetaData meta, String tn, String fName)
            throws SQLException {
        return meta.getColumns(null, null, tn.toLowerCase(), fName);
    }

    @Override
    protected ResultSet findPrimaryKeys(DatabaseMetaData meta, String tn) throws SQLException {
        return meta.getPrimaryKeys(null, null, tn.toLowerCase());
    }

    @Override
    protected ResultSet findIndex(DatabaseMetaData meta, String table, boolean unique)
            throws SQLException {
        return meta.getIndexInfo(null, null, table.toLowerCase(), unique, false);
    }

    @Override
    public void createQuery(APrint p, AQuery<?> query) {
        StringBuilder buffer = ((SqlDialectCreateContext) query.getContext()).getBuffer();

        if (p instanceof ALimit) {
            if (((ALimit) p).getLimit() >= 0)
                buffer.append(" LIMIT ").append(((ALimit) p).getLimit());
            if (((ALimit) p).getOffset() >= 0)
                buffer.append(" OFFSET ").append(((ALimit) p).getOffset());
        } else super.createQuery(p, query);
    }

    @Override
    protected void alterColumn(Statement sth, String tn, IConfig cfield) {

        String type = getDbType(cfield);

        String typeName = normalizeColumnName(cfield.getString("name", null));

        String sql = "ALTER TABLE " + tn + " ALTER COLUMN " + typeName + " TYPE " + type;

        String def = cfield.getExtracted("default");
        if (def != null) {
            def = getDbDef(def);
            sql = sql + ",ALTER COLUMN " + typeName + " SET DEFAULT " + def;
        }
        boolean notNull = cfield.getBoolean("notnull", false);
        sql = sql + ",ALTER COLUMN " + typeName + (notNull ? " SET NOT NULL" : " DROP NOT NULL");

        log().d("alter table", sql);
        try {
            sth.execute(sql);
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    @Override
    protected void dropIndex(Statement sth, String iName, String table) {
        String sql = "DROP INDEX " + iName;
        log().t(sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }
}
