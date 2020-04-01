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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.MRuntimeException;

/**
 * This class can compare a configuration with a database table structure and can modify the
 * database structure without deleting existing tables.
 *
 * <p>TODO: on request: remove other columns TODO: views, foreign keys TODO: data !!!
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DialectHsqldb extends DialectDefault {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /** {@inheritDoc} */
    @Override
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
     * getFieldConfigWithoutExtras.
     *
     * @param f a {@link de.mhus.lib.core.directory.ResourceNode} object.
     * @return a {@link java.lang.String} object.
     */
    protected String getFieldConfigWithoutExtras(IConfig f) {
        try {
            String type = getDbType(f);
            String ret = normalizeColumnName(f.getString("name")) + " " + type;

            //					String def = f.getExtracted("default");
            //					if (def != null) {
            //						def = getDbDef(def);
            //						ret = ret + " DEFAULT " + def;
            //					} else {
            //					    switch(getCaoType(f)) {
            //                        case BINARY:
            //                            break;
            //                        case BOOLEAN:
            //                            ret = ret + " DEFAULT FALSE";
            //                            break;
            //                        case DATETIME:
            //                            break;
            //                        case DOUBLE:
            //                            ret = ret + " DEFAULT 0";
            //                            break;
            //                        case ELEMENT:
            //                            break;
            //                        case LIST:
            //                            break;
            //                        case LONG:
            //                            ret = ret + " DEFAULT 0";
            //                            break;
            //                        case OBJECT:
            //                            break;
            //                        case STRING:
            //                            ret = ret + " DEFAULT ''";
            //                            break;
            //                        case TEXT:
            //                            ret = ret + " DEFAULT ''";
            //                            break;
            //                        default:
            //                            break;
            //					    }
            //					}
            //		boolean notNull = f.getBoolean("notnull", false);
            //		if (notNull)
            //			ret = ret + " NOT NULL";
            //		else
            //			ret = ret + " NULL";
            return ret;
        } catch (MException e) {
            throw new MRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toSqlDate(Date date) {
        synchronized (dateFormat) {
            return "TO_DATE('" + dateFormat.format(date) + "','YYYY-MM-DD HH:MI:SS')";
        }
    }

    /** {@inheritDoc} */
    @Override
    public String normalizeColumnName(String columnName) {
        return columnName.toUpperCase() + "_";
    }

    /** {@inheritDoc} */
    @Override
    protected void alterColumn(Statement sth, String tn, IConfig cfield) {
        String sql = "ALTER TABLE " + tn + " ALTER COLUMN " + getFieldConfigWithoutExtras(cfield);
        log().d("alter table", sql);
        try {
            sth.execute(sql);
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void alterTableChangePrimaryKey(Statement sth, String tn, String keys) {
        alterTableDropPrimaryKey(sth, tn);
        alterTableAddPrimaryKey(sth, tn, keys);
    }

    /** {@inheritDoc} */
    @Override
    protected void recreateIndex(
            Statement sth,
            boolean unique,
            boolean btree,
            String iName,
            String table,
            String columns) {

        iName = table + iName;
        String sql = "DROP INDEX " + iName;
        log().t(sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
        sql =
                "CREATE "
                        + (unique ? "UNIQUE" : "")
                        + " INDEX "
                        + iName
                        + (btree ? " USING BTREE" : "")
                        + " ON "
                        + table
                        + "("
                        + columns
                        + ")";
        log().t(sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void createIndex(
            Statement sth,
            boolean unique,
            boolean btree,
            String iName,
            String table,
            String columns) {

        iName = table + iName;

        String sql =
                "CREATE "
                        + (unique ? "UNIQUE" : "")
                        + " INDEX "
                        + iName
                        + (btree ? " USING BTREE" : "")
                        + " ON "
                        + table
                        + "("
                        + columns
                        + ")";
        log().t(sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected boolean equalsIndexName(String table, String iName, String iName2) {
        return iName2.equals(table + iName);
    }

    /** {@inheritDoc} */
    @Override
    public String normalizeTableName(String tableName) throws Exception {
        return tableName.toUpperCase() + "_";
    }

    /** {@inheritDoc} */
    @Override
    public String normalizeIndexName(String tableName, String tableOrg) throws Exception {
        return tableName.toUpperCase() + "_";
    }

    /** {@inheritDoc} */
    @Override
    public void prepareConnection(Connection con) throws SQLException {
        super.prepareConnection(con);
        Statement sth = con.createStatement();
        sth.execute("SET DATABASE TRANSACTION CONTROL MVCC");
        sth.execute("SET DATABASE DEFAULT ISOLATION LEVEL READ COMMITTED");
        // sth.execute("SET DATABASE TRANSACTION ROLLBACK ON DEADLOCK TRUE");
        sth.close();
    }
}
