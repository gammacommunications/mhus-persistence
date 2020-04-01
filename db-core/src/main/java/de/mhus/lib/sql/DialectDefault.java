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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.query.AAnd;
import de.mhus.lib.adb.query.AAttribute;
import de.mhus.lib.adb.query.ACompare;
import de.mhus.lib.adb.query.AConcat;
import de.mhus.lib.adb.query.ADbAttribute;
import de.mhus.lib.adb.query.ADynValue;
import de.mhus.lib.adb.query.AEnumFix;
import de.mhus.lib.adb.query.AFix;
import de.mhus.lib.adb.query.ALimit;
import de.mhus.lib.adb.query.AList;
import de.mhus.lib.adb.query.ALiteral;
import de.mhus.lib.adb.query.ALiteralList;
import de.mhus.lib.adb.query.ANot;
import de.mhus.lib.adb.query.ANull;
import de.mhus.lib.adb.query.AOperation;
import de.mhus.lib.adb.query.AOr;
import de.mhus.lib.adb.query.AOrder;
import de.mhus.lib.adb.query.APart;
import de.mhus.lib.adb.query.APrint;
import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.adb.query.ASubQuery;
import de.mhus.lib.cao.CaoMetaDefinition;
import de.mhus.lib.cao.util.MetadataBundle;
import de.mhus.lib.core.MSql;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.errors.NotSupportedException;

/**
 * This class can compare a configuration with a database table structure and can modify the
 * database structure without deleting existing tables.
 *
 * <p>TODO: on request: remove other columns TODO: views, foreign keys TODO: data !!!
 *
 * @author mikehummel
 */
public class DialectDefault extends Dialect {

    /**
     * Create or Update the defined tables. The config object need a bundle of 'table'
     * configurations which define the needed table structure. Example: [config] [table name='table
     * name' primary_key='field1,field2,...'] [field name='field name' prefix='prefix_' type='field
     * type' default='def value' notnull=yes/no /] [/table] [/config]
     *
     * <p>table: name primary_key
     *
     * <p>field: name type=INT,LONG,BOOL,DOUBLE,FLOAT,STRING,DATETIME,UUID,BLOB, UNKNOWN, BIGDECIMAL
     * size default notnull
     *
     * @param data
     * @param db
     * @param caoBundle
     * @param cleanup Cleanup unknown fields from the table
     * @throws Exception
     */
    @Override
    public void createTables(
            IConfig data, DbConnection db, MetadataBundle caoBundle, boolean cleanup)
            throws Exception {

        Connection con = ((JdbcConnection) db.instance()).getConnection();
        Statement sth = con.createStatement();
        DatabaseMetaData meta = con.getMetaData();

        // first check tables
        for (IConfig ctable : data.getNodes("table")) {
            String tName = ctable.getExtracted("name");
            String tPrefix = ctable.getExtracted("prefix", "");

            String tnOrg = tPrefix + tName;
            log().t("table", tnOrg);
            String tn = normalizeTableName(tnOrg);

            ResultSet tRes = findTable(meta, tn);

            if (caoBundle != null) caoBundle.getBundle().remove(tName);

            //			boolean found = false;
            //			while (tRes.next()) {
            //				String tn2 = tRes.getString("TABLE_NAME");
            //				if (tn.equals(tn2)) {
            //					found = true;
            //				}
            //			}

            if (tRes.next()) {
                // merge table definition
                log().t("--- found table", tName);

                MutableMetadata caoMeta = null;
                if (caoBundle != null) {
                    caoMeta = new MutableMetadata();
                    caoBundle.getBundle().put(tName, caoMeta);
                }

                // check fields

                LinkedList<String> fieldsInTable = null;
                if (cleanup) fieldsInTable = new LinkedList<>();

                for (IConfig cfield : ctable.getNodes("field")) {

                    String fNameOrg = cfield.getExtracted("name");
                    String fName = normalizeColumnName(fNameOrg);

                    if (cfield.getString(K_CATEGORIES, "").indexOf(C_VIRTUAL) < 0) {
                        ResultSet fRes = findColumn(meta, tn, fName);
                        log().t("field", tName, fNameOrg);
                        if (fRes.next()) {
                            String fName2 = fRes.getString("COLUMN_NAME");
                            String fType = fRes.getString("TYPE_NAME");
                            int fSize = fRes.getInt("COLUMN_SIZE");
                            int fNull = fRes.getInt("NULLABLE");
                            String fDef = fRes.getString("COLUMN_DEF");
                            log().t("found field", tName, fName2, fType, fSize, fNull, fDef);

                            // check field type && not null

                            String fType1 = getDbType(cfield);

                            if (!equalTypes(fType1, fType, fSize)) {
                                alterColumn(sth, tn, cfield);
                            } else {
                                boolean xdef = cfield.getProperty("default") != null;
                                // check field default
                                if (fDef != null && !xdef) {
                                    // remove default
                                    alterColumnDropDefault(sth, tn, fName);
                                } else if (fDef == null && xdef
                                        || fDef != null
                                                && !fDef.equals(cfield.getProperty("default"))) {
                                    // set default
                                    alterColumnSetDefault(sth, tn, fName, cfield);
                                }
                            }

                        } else {
                            alterColumnAdd(sth, tn, cfield);
                        }
                        fRes.close();

                        if (fieldsInTable != null)
                            fieldsInTable.add(fName); // remember not to remove
                    }
                    if (caoMeta != null) {
                        List<CaoMetaDefinition> metaMap = caoMeta.getMap();
                        CaoMetaDefinition.TYPE caoType = getCaoType(cfield);
                        String[] categories =
                                MString.splitIgnoreEmpty(
                                        cfield.getString(K_CATEGORIES, ""), ",", true);
                        metaMap.add(
                                new CaoMetaDefinition(
                                        caoMeta,
                                        cfield.getExtracted("name"),
                                        caoType,
                                        cfield.getExtracted("nls"),
                                        cfield.getInt("size", 100),
                                        categories));
                    }
                }

                // END fields

                if (tRes.next()) {
                    log().t("*** found more then one table", tName);
                }

                // remove fields
                if (fieldsInTable != null) {
                    ResultSet fRes = meta.getColumns(null, null, tn, null);
                    while (fRes.next()) {
                        String fName2 = fRes.getString("COLUMN_NAME");
                        if (!fieldsInTable.contains(fName2)) {
                            log().t("remove column", fName2);
                            alterColumnDrop(sth, tn, fName2);
                        }
                    }
                }

            } else {
                log().t("--- table not found", tName);
                // create

                MutableMetadata caoMeta = null;
                if (caoBundle != null) {
                    caoMeta = new MutableMetadata();
                    caoBundle.getBundle().put(tName, caoMeta);
                }

                createTable(sth, tn, ctable);
                for (IConfig f : ctable.getNodes("field")) {
                    if (caoMeta != null) {
                        List<CaoMetaDefinition> metaMap = caoMeta.getMap();
                        CaoMetaDefinition.TYPE caoType = getCaoType(f);
                        metaMap.add(
                                new CaoMetaDefinition(
                                        caoMeta,
                                        f.getExtracted("name"),
                                        caoType,
                                        f.getExtracted("nls"),
                                        f.getInt("size", 100)));
                    }
                }
            }
            tRes.close();

            // check primary key

            String keys = ctable.getExtracted("primary_key");
            // order by name
            if (keys != null) {
                TreeSet<String> set = new TreeSet<String>();
                for (String item : keys.split(",")) set.add(normalizeColumnName(item));
                keys = MString.join(set.iterator(), ",");
            }

            // look for the primary key
            tRes = findPrimaryKeys(meta, tn);
            String keys2 = null;
            while (tRes.next()) {
                if (keys2 == null) keys2 = tRes.getString("COLUMN_NAME");
                else keys2 = keys2 + "," + tRes.getString("COLUMN_NAME");
            }
            tRes.close();
            if (keys2 != null) {
                log().t("found primary key", keys2);
                if (keys == null) {
                    alterTableDropPrimaryKey(sth, tn);
                } else if (!keys.equals(keys2)) {
                    alterTableChangePrimaryKey(sth, tn, keys);
                }
            } else {
                if (keys != null) {
                    alterTableAddPrimaryKey(sth, tn, keys);
                }
            }

            con.commit();
        }
        sth.close();
    }

    protected boolean equalTypes(String should, String is, int fSize) {
        is = is.toUpperCase();
        if (is.equals("INTEGER") && should.equals("INT")) return true;
        if (is.indexOf("CHAR") >= 0) {
            is = is + "(" + fSize + ")"; // add size to type
        }
        return should.equals(is);
    }

    protected ResultSet findPrimaryKeys(DatabaseMetaData meta, String tn) throws SQLException {
        return meta.getPrimaryKeys(null, null, tn);
    }

    protected ResultSet findColumn(DatabaseMetaData meta, String tn, String fName)
            throws SQLException {
        return meta.getColumns(null, null, tn, fName);
    }

    protected ResultSet findTable(DatabaseMetaData meta, String name) throws SQLException {
        return meta.getTables(null, null, name, new String[] {"TABLE"});
    }

    protected void createTable(Statement sth, String tn, IConfig ctable) {
        StringBuilder sql = new StringBuilder();
        sql.append("create table " + tn + " ( ");
        boolean first = true;
        for (IConfig f : ctable.getNodes("field")) {
            if (!first) sql.append(",");
            sql.append(getFieldConfig(f));
            first = false;
        }
        sql.append(" )");
        createTableLastCheck(ctable, tn, sql);
        log().d(sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void createTableLastCheck(IConfig ctable, String tn, StringBuilder sql) {}

    protected void alterTableAddPrimaryKey(Statement sth, String tn, String keys) {
        String sql = "ALTER TABLE " + tn + " ADD PRIMARY KEY(" + keys + ")";
        log().d("new primary key", sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void alterTableChangePrimaryKey(Statement sth, String tn, String keys) {
        String sql = "ALTER TABLE " + tn + " DROP PRIMARY KEY, ADD PRIMARY KEY(" + keys + ")";
        log().d("new primary key", sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void alterTableDropPrimaryKey(Statement sth, String tn) {
        String sql = "ALTER TABLE " + tn + " DROP PRIMARY KEY";
        log().d("drop primary key", sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void alterColumnAdd(Statement sth, String tn, IConfig cfield) {
        //		String sql = "ALTER TABLE " + tn + " ADD COLUMN (" + getFieldConfig(cfield) + ")";
        String sql = "ALTER TABLE " + tn + " ADD COLUMN " + getFieldConfig(cfield);
        log().d("alter table", sql);
        try {
            sth.execute(sql);
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void alterColumnSetDefault(Statement sth, String tn, String fName, IConfig cfield) {
        String sql = null;
        try {
            sql =
                    "ALTER TABLE "
                            + tn
                            + " ALTER COLUMN "
                            + fName
                            + " SET DEFAULT "
                            + getDbDef(cfield.getString("default", null));
            log().d("alter table", sql);
            sth.execute(sql);
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void alterColumnDropDefault(Statement sth, String tn, String fName) {
        String sql = "ALTER TABLE " + tn + " ALTER COLUMN " + fName + " DROP DEFAULT";
        log().d("alter table", sql);
        try {
            sth.execute(sql);
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void alterColumn(Statement sth, String tn, IConfig cfield) {
        String sql = "ALTER TABLE " + tn + " MODIFY COLUMN " + getFieldConfig(cfield);
        log().d("alter table", sql);
        try {
            sth.execute(sql);
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void alterColumnDrop(Statement sth, String tn, String fName) {
        String sql = "ALTER TABLE " + tn + " DROP COLUMN " + fName;
        log().d("alter table", sql);
        try {
            sth.execute(sql);
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    /**
     * Create or update indexes. The configuration need a bundle of 'index' elements to define the
     * indexes. Example: [config] [index name='name' table='table name' btree=yes/no unique=yes/no
     * fields='field1,field2,...'/] [/config]
     *
     * @param data
     * @param db
     * @param caoMeta
     * @throws Exception
     */
    @Override
    public void createIndexes(
            IConfig data, DbConnection db, MetadataBundle caoMeta, boolean cleanup)
            throws Exception {

        Connection con = ((JdbcConnection) db.instance()).getConnection();
        Statement sth = con.createStatement();
        DatabaseMetaData meta = con.getMetaData();

        // first check tables
        for (IConfig cindex : data.getNodes("index")) {
            String iNameOrg = cindex.getExtracted("name");
            String tableName = cindex.getExtracted("table");
            String prefix = cindex.getExtracted("prefix", "");
            String tableOrg = prefix + tableName;
            String iName = normalizeIndexName(iNameOrg, tableOrg);
            String table = normalizeTableName(tableOrg);
            // String type    = cindex.getExtracted("type");
            boolean btree = cindex.getBoolean("btree", false);
            String columnsOrg = cindex.getExtracted("fields");
            String columns = null;
            // order by name, trim, normalize
            if (columnsOrg != null) {
                TreeSet<String> set = new TreeSet<String>();
                for (String item : columnsOrg.split(",")) set.add(normalizeColumnName(item.trim()));
                columns = MString.join(set.iterator(), ",");
            } else {
                columns = ""; // ?
            }

            boolean unique = cindex.getBoolean("unique", false);

            String columns2 = null;
            {
                ResultSet res = findIndex(meta, table, unique);
                while (res.next()) {

                    String iName2 = res.getString("INDEX_NAME");
                    String fName2 = res.getString("COLUMN_NAME");
                    if (iName2 != null && fName2 != null) {
                        if (equalsIndexName(table, iName, iName2)) {
                            if (columns2 == null) columns2 = fName2;
                            else columns2 = columns2 + "," + fName2;
                        }
                    }
                }
                res.close();
            }
            boolean doubleExists = false;
            {
                ResultSet res = findIndex(meta, table, !unique);
                while (res.next()) {

                    String iName2 = res.getString("INDEX_NAME");
                    String fName2 = res.getString("COLUMN_NAME");
                    if (iName2 != null && fName2 != null) {
                        if (equalsIndexName(table, iName, iName2)) {
                            doubleExists = true;
                            break;
                        }
                    }
                }
                res.close();
            }
            if (columns2 == null) {
                // create index
                log().d("create index", doubleExists, iNameOrg, columnsOrg);
                if (doubleExists) recreateIndex(sth, unique, btree, iName, table, columns);
                else createIndex(sth, unique, btree, iName, table, columns);
            } else {

                if (!columns.equals(columns2)) {
                    log().d("recreate index", doubleExists, iName, columns2, columns);
                    recreateIndex(sth, unique, btree, iName, table, columns);
                }
            }

            con.commit();
        }
        sth.close();
    }

    protected ResultSet findIndex(DatabaseMetaData meta, String table, boolean unique)
            throws SQLException {
        return meta.getIndexInfo(null, null, table, unique, false);
    }

    protected boolean equalsIndexName(String table, String iName, String iName2) {
        return iName2.equals(iName);
    }

    protected void dropIndex(Statement sth, String iName, String table) {
        String sql = "DROP INDEX " + iName + " ON " + table;
        log().t(sql);
        try {
            sth.execute(sql.toString());
        } catch (Exception e) {
            log().e(sql, e);
        }
    }

    protected void recreateIndex(
            Statement sth,
            boolean unique,
            boolean btree,
            String iName,
            String table,
            String columns) {
        dropIndex(sth, iName, table);
        createIndex(sth, unique, btree, iName, table, columns);
    }

    protected void createIndex(
            Statement sth,
            boolean unique,
            boolean btree,
            String iName,
            String table,
            String columns) {
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

    /**
     * Execute 'data' configs: select = a select query to define a condition and/or data set set and
     * column = set a value in the config to the value from column condition = found,not
     * found,error,no error
     *
     * @param data
     * @param db
     * @throws Exception
     */
    @Override
    public void createData(IConfig data, DbConnection db) throws Exception {
        Connection con = ((JdbcConnection) db.instance()).getConnection();
        Statement sth = con.createStatement();

        // first check tables
        for (IConfig cdata : data.getNodes("data")) {
            // String table  = cdata.getExtracted("table");
            String select = cdata.getExtracted("select");
            String set = cdata.getExtracted("set");
            String column = cdata.getExtracted("column");
            String condition = cdata.getExtracted("condition");

            boolean foundRow = false;
            boolean foundError = false;
            if (select != null) {
                log().t("select", select);
                try {
                    ResultSet res = sth.executeQuery(select);
                    if (res.next()) {
                        if (set != null && column != null) {
                            data.setProperty(set, column);
                        }
                        foundRow = true;
                    }
                    res.close();
                } catch (Exception e) {
                    log().e(select, e);
                    foundError = true;
                }
            }

            boolean accepted = true;

            if (condition != null) {
                accepted =
                        (condition.equals("found") && foundRow)
                                || (condition.equals("not found") && !foundRow)
                                || (condition.equals("error") && foundError)
                                || (condition.equals("no error") && !foundError);
            }

            if (accepted) {
                for (IConfig cexecute : cdata.getNodes("execute")) {
                    String sql = cexecute.getExtracted("sql");
                    if (sql != null) {
                        log().t("execute", sql);
                        try {
                            sth.execute(sql.toString());
                        } catch (Exception e) {
                            log().e(sql, e);
                        }
                    }
                }
            }
        }
        sth.close();
    }

    @Override
    public String normalizeTableName(String tableName) throws Exception {
        return tableName + "_";
    }

    @Override
    public String normalizeIndexName(String tableName, String tableOrg) throws Exception {
        return tableName;
    }

    @Override
    public String normalizeColumnName(String columnName) {
        return columnName;
    }

    @Override
    public String escape(String text) {
        return MSql.escape(text);
    }

    @Override
    public void createQuery(APrint p, AQuery<?> query) {
        StringBuilder buffer = ((SqlDialectCreateContext) query.getContext()).getBuffer();

        if (p instanceof AQuery) {
            //		buffer.append('(');
            {
                boolean first = true;
                for (AOperation operation : ((AQuery<?>) p).getOperations()) {
                    if (operation instanceof APart) {
                        if (first) first = false;
                        else buffer.append(" and ");
                        createQuery(operation, query);
                    }
                }
            }
            //		buffer.append(')');

            {
                boolean first = true;
                AOperation limit = null;
                for (AOperation operation : ((AQuery<?>) p).getOperations()) {
                    if (operation instanceof AOrder) {
                        if (first) {
                            first = false;
                            buffer.append(" ORDER BY ");
                        } else buffer.append(" , ");
                        createQuery(operation, query);
                    } else if (operation instanceof ALimit) limit = operation;
                }

                if (limit != null) {
                    createQuery(limit, query);
                }
            }
        } else if (p instanceof AAnd) {
            buffer.append('(');
            boolean first = true;
            for (APart part : ((AAnd) p).getOperations()) {
                if (first) first = false;
                else buffer.append(" and ");
                createQuery(part, query);
            }
            buffer.append(')');
        } else if (p instanceof ACompare) {
            createQuery(((ACompare) p).getLeft(), query);
            switch (((ACompare) p).getEq()) {
                case EG:
                    buffer.append(" => ");
                    break;
                case EL:
                    buffer.append(" <= ");
                    break;
                case EQ:
                    buffer.append(" = ");
                    break;
                case GT:
                    buffer.append(" > ");
                    break;
                case GE:
                    buffer.append(" >= ");
                    break;
                case LIKE:
                    buffer.append(" like ");
                    break;
                case LT:
                    buffer.append(" < ");
                    break;
                case LE:
                    buffer.append(" <= ");
                    break;
                case NE:
                    buffer.append(" <> ");
                    break;
                case IN:
                    buffer.append(" in ");
                    break;
            }
            createQuery(((ACompare) p).getRight(), query);
        } else if (p instanceof AConcat) {
            buffer.append("concat(");
            boolean first = true;
            for (AAttribute part : ((AConcat) p).getParts()) {
                if (first) first = false;
                else buffer.append(",");
                createQuery(part, query);
            }
            buffer.append(")");
        } else if (p instanceof ADbAttribute) {
            Class<?> c = ((ADbAttribute) p).getClazz();
            if (c == null) c = query.getType();
            DbManager manager = ((SqlDialectCreateContext) query.getContext()).getManager();
            String name =
                    "db." + manager.getMappingName(c) + "." + ((ADbAttribute) p).getAttribute();
            if (manager.getNameMapping().get(name) == null) log().w("mapping not exist", name);
            buffer.append("$").append(name).append('$');
        } else if (p instanceof ADynValue) {
            DbManager manager = ((SqlDialectCreateContext) query.getContext()).getManager();
            buffer.append('$').append(((ADynValue) p).getDefinition(manager)).append('$');
        } else if (p instanceof AEnumFix) {
            buffer.append(((AEnumFix) p).getValue().ordinal());
        } else if (p instanceof AFix) {
            buffer.append(((AFix) p).getValue());
        } else if (p instanceof ALimit) {
            buffer.append(" LIMIT ")
                    .append(((ALimit) p).getOffset())
                    .append(",")
                    .append(((ALimit) p).getLimit()); // mysql specific !!
        } else if (p instanceof AList) {
            buffer.append('(');
            boolean first = true;
            for (AAttribute part : ((AList) p).getOperations()) {
                if (first) first = false;
                else buffer.append(",");
                createQuery(part, query);
            }
            buffer.append(')');
        } else if (p instanceof ALiteral) {
            buffer.append(((ALiteral) p).getLiteral());
        } else if (p instanceof ALiteralList) {
            for (APart part : ((ALiteralList) p).getOperations()) {
                createQuery(part, query);
            }
        } else if (p instanceof ANot) {
            buffer.append("not ");
            createQuery(((ANot) p).getOperation(), query);
        } else if (p instanceof ANull) {
            createQuery(((ANull) p).getAttr(), query);
            buffer.append(" is ");
            if (!((ANull) p).isIs()) buffer.append("not ");
            buffer.append("null");
        } else if (p instanceof AOr) {
            buffer.append('(');
            boolean first = true;
            for (APart part : ((AOr) p).getOperations()) {
                if (first) first = false;
                else buffer.append(" or ");
                createQuery(part, query);
            }
            buffer.append(')');
        } else if (p instanceof AOrder) {
            DbManager manager = ((SqlDialectCreateContext) query.getContext()).getManager();
            buffer.append("$db.")
                    .append(manager.getMappingName(((AOrder) p).getClazz()))
                    .append('.')
                    .append(((AOrder) p).getAttribute())
                    .append('$');
            buffer.append(' ').append(((AOrder) p).isAsc() ? "ASC" : "DESC");
        } else if (p instanceof ASubQuery) {
            DbManager manager = ((SqlDialectCreateContext) query.getContext()).getManager();
            String qualification = manager.toQualification(((ASubQuery) p).getSubQuery()).trim();

            createQuery(((ASubQuery) p).getLeft(), query);
            buffer.append(" IN (");

            StringBuilder buffer2 = new StringBuilder().append("DISTINCT ");

            AQuery<?> subQuery = ((ASubQuery) p).getSubQuery();
            subQuery.setContext(new SqlDialectCreateContext(manager, buffer2));
            createQuery(((ASubQuery) p).getProjection(), subQuery);

            buffer.append(
                    manager.createSqlSelect(
                            ((ASubQuery) p).getSubQuery().getType(),
                            buffer2.toString(),
                            qualification));

            buffer.append(")");
        } else throw new NotSupportedException(p.getClass());
    }

    @Override
    public String toBoolValue(boolean value) {
        return value ? "1" : "0";
    }
}
