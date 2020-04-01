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
package de.mhus.lib.adb.model;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.annotations.adb.DbIndex;
import de.mhus.lib.annotations.adb.DbIndex.TYPE;
import de.mhus.lib.annotations.adb.DbPersistent;
import de.mhus.lib.annotations.adb.DbPrimaryKey;
import de.mhus.lib.annotations.adb.DbTable;
import de.mhus.lib.annotations.adb.DbType;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.config.HashConfig;
import de.mhus.lib.core.config.MConfig;
import de.mhus.lib.core.directory.ResourceNode;
import de.mhus.lib.core.directory.WritableResourceNode;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.core.lang.Raw;
import de.mhus.lib.core.util.MUri;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.NotFoundException;
import de.mhus.lib.sql.DbConnection;
import de.mhus.lib.sql.DbPrepared;
import de.mhus.lib.sql.DbResult;
import de.mhus.lib.sql.Dialect;

/**
 * Abstract Table class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public abstract class Table extends MObject {

    protected Class<?> clazz;
    protected String registryName;
    protected DbManager manager;
    protected String name;
    protected String tableNameOrg;
    protected DbSchema schema;
    protected String tableName;
    protected HashMap<String, Field> fIndex = new HashMap<String, Field>();
    protected LinkedList<Field> fList = new LinkedList<Field>();
    protected LinkedList<FieldRelation> relationList = new LinkedList<FieldRelation>();
    protected HashMap<String, FieldRelation> relationIndex = new HashMap<String, FieldRelation>();
    private HashMap<String, IndexStruc> iIdx = new HashMap<String, IndexStruc>();
    protected LinkedList<Field> pk = new LinkedList<Field>();
    private DbPrepared sqlPrimary;
    private DbPrepared sqlInsert;
    private DbPrepared sqlUpdate;
    private DbPrepared sqlUpdateForce;
    private DbPrepared sqlDelete;
    private LinkedList<Feature> features = new LinkedList<Feature>();
    protected ResourceNode<?> attributes;

    /**
     * init.
     *
     * @param manager a {@link de.mhus.lib.adb.DbManager} object.
     * @param clazz a {@link java.lang.Class} object.
     * @param registryName a {@link java.lang.String} object.
     * @param tableName a {@link java.lang.String} object.
     */
    public void init(DbManager manager, Class<?> clazz, String registryName, String tableName) {
        this.manager = manager;
        this.schema = manager.getSchema();
        this.clazz = clazz;
        this.registryName = registryName;
        this.tableName = tableName;
    }

    /**
     * initDatabase.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param cleanup a boolean.
     * @throws java.lang.Exception if any.
     */
    public void initDatabase(DbConnection con, boolean cleanup) throws Exception {

        DbTable table = MSystem.findAnnotation(clazz, DbTable.class);
        if (tableName != null) {
            name = tableName;
        } else if (table == null || MString.isEmptyTrim(table.tableName())) {
            name = clazz.getSimpleName();
        } else {
            name = table.tableName();
        }

        if (table != null && !MString.isEmptyTrim(table.attributes())) {
            attributes = MConfig.toConfig(table.attributes());
        } else {
            attributes = new HashConfig();
        }

        tableNameOrg = schema.getTableName(name);
        tableName = manager.getPool().getDialect().normalizeTableName(tableNameOrg);

        log().t("new table", name, tableName);

        parseFields();

        // features
        if (table != null) {
            for (String featureName : table.features()) {
                Feature feature = manager.getSchema().createFeature(manager, this, featureName);
                if (feature != null) features.add(feature);
            }
        }

        createTable(con, cleanup);
        postInit();
    }

    /**
     * parseFields.
     *
     * @throws java.lang.Exception if any.
     */
    protected abstract void parseFields() throws Exception;

    /**
     * addToIndex.
     *
     * @param list an array of {@link java.lang.String} objects.
     * @param field a {@link de.mhus.lib.adb.model.Field} object.
     */
    protected void addToIndex(String[] list, DbIndex.TYPE type, String hints, Field field) {
        for (String name : list) {
            IndexStruc list2 = iIdx.get(name);
            if (list2 == null) {
                list2 = new IndexStruc();
                iIdx.put(name, list2);
            }
            list2.name = name;
            list2.hints = hints;
            list2.type = type;
            if (field != null) list2.add(field);
        }
    }

    /**
     * addField.
     *
     * @param field a {@link de.mhus.lib.adb.model.FieldRelation} object.
     */
    protected void addField(FieldRelation field) {
        relationList.add(field);
        relationIndex.put(field.getName(), field);
    }

    /**
     * getFieldRelation.
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.adb.model.FieldRelation} object.
     */
    public FieldRelation getFieldRelation(String name) {
        return relationIndex.get(name);
    }

    /**
     * addField.
     *
     * @param field a {@link de.mhus.lib.adb.model.Field} object.
     */
    protected void addField(Field field) {
        field.table = this;
        fIndex.put(field.createName, field);
        fList.add(field);
        if (field.isPrimary && field.isPersistent()) pk.add(field);
    }

    /**
     * fillNameMapping.
     *
     * @param nameMapping a {@link java.util.HashMap} object.
     * @throws java.lang.Exception if any.
     */
    public void fillNameMapping(HashMap<String, Object> nameMapping) throws Exception {
        nameMapping.put("db." + manager.getMappingName(clazz), new Raw(tableName));
        for (Field f : fList) {
            f.fillNameMapping(nameMapping);
        }
    }

    /**
     * prepareCreate.
     *
     * @param object a {@link java.lang.Object} object.
     * @throws java.lang.Exception if any.
     */
    public void prepareCreate(Object object) throws Exception {
        for (Field f : fList) {
            f.prepareCreate(object);
        }
        for (FieldRelation f : relationList) {
            f.prepareCreate(object);
        }
    }

    /**
     * createObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param object a {@link java.lang.Object} object.
     * @throws java.lang.Exception if any.
     */
    public void createObject(DbConnection con, Object object) throws Exception {

        for (Feature f : features) f.preCreateObject(con, object);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        for (Field f : fList) {
            attributes.put(f.name, f.getFromTarget(object));
        }

        schema.internalCreateObject(con, name, object, attributes);

        sqlInsert.getStatement(con).execute(attributes);

        for (Feature f : features) f.postCreateObject(con, object);

        for (FieldRelation f : relationList) {
            f.created(con, object);
        }
    }

    /**
     * saveObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param object a {@link java.lang.Object} object.
     * @throws java.lang.Exception if any.
     */
    public void saveObject(DbConnection con, Object object) throws Exception {

        for (Feature f : features) f.preSaveObject(con, object);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        for (Field f : fList) {
            attributes.put(f.name, f.getFromTarget(object));
        }

        for (FieldRelation f : relationList) {
            f.prepareSave(con, object);
        }

        schema.internalSaveObject(con, name, object, attributes);

        int c = sqlUpdate.getStatement(con).executeUpdate(attributes);
        if (c != 1) throw new MException("update failed, updated objects " + c);

        for (Feature f : features) f.postSaveObject(con, object);

        for (FieldRelation f : relationList) {
            f.saved(con, object);
        }
    }

    /**
     * saveObjectForce.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param object a {@link java.lang.Object} object.
     * @param raw a boolean.
     * @throws java.lang.Exception if any.
     */
    public void saveObjectForce(DbConnection con, Object object, boolean raw) throws Exception {

        manager.getSchema().authorizeSaveForceAllowed(con, this, object, raw);

        if (!raw) for (Feature f : features) f.preSaveObject(con, object);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        for (Field f : fList) {
            attributes.put(f.name, f.getFromTarget(object));
        }

        for (FieldRelation f : relationList) {
            f.prepareSave(con, object);
        }

        schema.internalSaveObject(con, name, object, attributes);

        int c = sqlUpdateForce.getStatement(con).executeUpdate(attributes);
        if (c != 1) throw new MException("update failed, updated objects " + c);

        if (!raw) for (Feature f : features) f.postSaveObject(con, object);

        for (FieldRelation f : relationList) {
            f.saved(con, object);
        }
    }

    /**
     * updateAttributes.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param object a {@link java.lang.Object} object.
     * @param raw a boolean.
     * @param attributeNames a {@link java.lang.String} object.
     * @throws java.lang.Exception if any.
     */
    public void updateAttributes(
            DbConnection con, Object object, boolean raw, String... attributeNames)
            throws Exception {

        manager.getSchema().authorizeUpdateAttributes(con, this, object, raw, attributeNames);

        HashMap<String, Object> attributes = new HashMap<String, Object>();

        // prepare object
        if (!raw) for (Feature f : features) f.preSaveObject(con, object);

        // create query and collect values

        StringBuilder sql = new StringBuilder().append("UPDATE ").append(tableName).append(" SET ");
        int nr = 0;
        for (String aname : attributeNames) {
            Field f = fIndex.get(aname);
            if (f == null) throw new NotFoundException("field not found", name, aname);

            if (!f.isPrimary && f.isPersistent()) {
                if (nr > 0) sql.append(",");
                sql.append(f.name).append("=$").append(f.name).append("$");
                nr++;
                attributes.put(f.name, f.getFromTarget(object)); // collect values
            }
        }
        if (nr == 0) throw new NotFoundException("no valid fields found");

        sql.append(" WHERE ");
        nr = 0;
        for (Field f : pk) {
            sql.append((nr > 0 ? " AND " : ""))
                    .append(f.name)
                    .append("=$")
                    .append(f.name)
                    .append("$");
            nr++;
            attributes.put(f.name, f.getFromTarget(object)); // collect values
        }

        DbPrepared query = manager.getPool().createStatement(sql.toString());

        // execute query

        // not needed - object itself is not saved
        //		for (FieldRelation f : relationList) {
        //			f.prepareSave(con,object);
        //		}

        schema.internalSaveObject(con, name, object, attributes);

        int c = query.getStatement(con).executeUpdate(attributes);
        if (c != 1) throw new MException("update failed, updated objects " + c);

        if (!raw) for (Feature f : features) f.postSaveObject(con, object);

        // not needed - object itself is not saved
        //		for (FieldRelation f : relationList) {
        //			f.saved(con,object);
        //		}

    }

    /**
     * postInit.
     *
     * @throws de.mhus.lib.errors.MException if any.
     */
    protected void postInit() throws MException {

        Collections.sort(
                pk,
                new Comparator<Field>() {

                    @Override
                    public int compare(Field o1, Field o2) {
                        return o1.name.compareTo(o2.name);
                    }
                });

        String sql = "SELECT * FROM " + tableName + " WHERE ";
        int nr = 0;
        for (Field f : pk) {
            sql += (nr > 0 ? " AND " : "") + f.name + "=$" + nr + "$";
            nr++;
        }
        // TODO dialect.appendSqlLimit(0,1)

        sqlPrimary = manager.getPool().createStatement(sql);

        // ------

        sql = "INSERT INTO " + tableName + " (";
        nr = 0;
        for (Field f : fList) {
            if (f.isPersistent()) {
                if (nr > 0) sql += ",";
                sql += f.name;
                nr++;
            }
        }
        sql += ") VALUES (";
        nr = 0;
        for (Field f : fList) {
            if (f.isPersistent()) {
                if (nr > 0) sql += ",";
                sql += "$" + f.name + "$";
                nr++;
            }
        }
        sql += ")";

        sqlInsert = manager.getPool().createStatement(sql);

        // ------

        sql = "UPDATE " + tableName + " SET ";
        nr = 0;
        for (Field f : fList) {
            if (!f.isPrimary && f.isPersistent() && !f.isReadOnly()) {
                if (nr > 0) sql += ",";
                sql += f.name + "=$" + f.name + "$";
                nr++;
            }
        }
        sql += " WHERE ";
        nr = 0;
        for (Field f : pk) {
            sql += (nr > 0 ? " AND " : "") + f.name + "=$" + f.name + "$";
            nr++;
        }

        sqlUpdate = manager.getPool().createStatement(sql);

        // ------

        sql = "UPDATE " + tableName + " SET ";
        nr = 0;
        for (Field f : fList) {
            if (!f.isPrimary && f.isPersistent()) {
                if (nr > 0) sql += ",";
                sql += f.name + "=$" + f.name + "$";
                nr++;
            }
        }
        sql += " WHERE ";
        nr = 0;
        for (Field f : pk) {
            sql += (nr > 0 ? " AND " : "") + f.name + "=$" + f.name + "$";
            nr++;
        }

        sqlUpdateForce = manager.getPool().createStatement(sql);

        // ------

        sql = "DELETE FROM " + tableName + " WHERE ";
        nr = 0;
        for (Field f : pk) {
            sql += (nr > 0 ? " AND " : "") + f.name + "=$" + f.name + "$";
            nr++;
        }

        sqlDelete = manager.getPool().createStatement(sql);
    }

    /**
     * getObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param keys an array of {@link java.lang.Object} objects.
     * @return a {@link java.lang.Object} object.
     * @throws java.lang.Exception if any.
     */
    public Object getObject(DbConnection con, Object[] keys) throws Exception {

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        int nr = 0;
        for (Object key : keys) {
            attributes.put(String.valueOf(nr), key);
            nr++;
        }
        DbResult ret = sqlPrimary.getStatement(con).executeQuery(attributes);
        if (!ret.next()) {
            ret.close();
            return null;
        }

        for (Feature f : features) f.preGetObject(con, ret);

        Object obj = schema.createObject(clazz, registryName, ret, manager, true);

        // fill object
        for (Field f : fList) {
            f.setToTarget(ret, obj);
        }

        for (Feature f : features) f.postGetObject(con, obj);

        for (FieldRelation f : relationList) {
            f.loaded(con, obj);
        }

        return obj;
    }

    /**
     * existsObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param keys an array of {@link java.lang.Object} objects.
     * @return a boolean.
     * @throws java.lang.Exception if any.
     */
    public boolean existsObject(DbConnection con, Object[] keys) throws Exception {

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        int nr = 0;
        for (Object key : keys) {
            attributes.put(String.valueOf(nr), key);
            nr++;
        }
        DbResult ret = sqlPrimary.getStatement(con).executeQuery(attributes);
        if (!ret.next()) {
            ret.close();
            return false;
        }
        ret.close();

        return true;
    }

    /**
     * injectObject.
     *
     * @param obj a {@link java.lang.Object} object.
     */
    public void injectObject(Object obj) {
        for (FieldRelation f : relationList) {
            f.inject(obj);
        }
    }

    /**
     * fillObject.
     *
     * @param obj a {@link java.lang.Object} object.
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param res a {@link de.mhus.lib.sql.DbResult} object.
     * @throws java.lang.Throwable if any.
     */
    public void fillObject(Object obj, DbConnection con, DbResult res) throws Throwable {

        for (Feature f : features) f.preFillObject(obj, con, res);

        for (Field f : fList) {
            try {
                f.setToTarget(res, obj);
            } catch (Throwable t) {
                manager.getSchema().onFillObjectException(Table.this, obj, res, f, t);
            }
        }

        for (Feature f : features) f.postFillObject(obj, con);

        for (FieldRelation f : relationList) {
            f.loaded(con, obj);
        }
    }

    /**
     * fillObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param obj a {@link java.lang.Object} object.
     * @param keys an array of {@link java.lang.Object} objects.
     * @return a {@link java.lang.Object} object.
     * @throws java.lang.Throwable if any.
     */
    public Object fillObject(DbConnection con, Object obj, Object[] keys) throws Throwable {

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        int nr = 0;
        for (Object key : keys) {
            attributes.put(String.valueOf(nr), key);
            nr++;
        }
        DbResult ret = sqlPrimary.getStatement(con).executeQuery(attributes);
        if (!ret.next()) {
            ret.close();
            return null;
        }

        for (Feature f : features) f.preFillObject(obj, con, ret);

        // fill object
        for (Field f : fList) {
            try {
                f.setToTarget(ret, obj);
            } catch (Throwable t) {
                manager.getSchema().onFillObjectException(Table.this, obj, ret, f, t);
            }
        }
        ret.close();

        for (Feature f : features) f.postFillObject(obj, con);

        for (FieldRelation f : relationList) {
            f.loaded(con, obj);
        }

        return obj;
    }

    /**
     * objectChanged.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param obj a {@link java.lang.Object} object.
     * @param keys an array of {@link java.lang.Object} objects.
     * @return a boolean.
     * @throws java.lang.Exception if any.
     */
    public boolean objectChanged(DbConnection con, Object obj, Object[] keys) throws Exception {

        for (FieldRelation field : relationList) {
            log().d("relation changed", getName(), field, field.getName());
            if (field.isChanged(obj)) return true;
        }

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        int nr = 0;
        for (Object key : keys) {
            attributes.put(String.valueOf(nr), key);
            nr++;
        }
        DbResult ret = sqlPrimary.getStatement(con).executeQuery(attributes);
        if (!ret.next()) {
            ret.close();
            log().d("row not found");
            return true;
        }

        //		for (Feature f : features)
        //			f.fillObject(obj, con, ret);

        // check object
        for (Field f : fList) {
            if (!f.isTechnical() && f.changed(ret, obj)) {
                ret.close();
                log().d("changed field", getName(), f, f.getName());
                return true;
            }
        }
        ret.close();
        return false;
    }

    /**
     * Create the dables in the database.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param cleanup a boolean.
     * @throws java.lang.Exception if any.
     */
    public void createTable(DbConnection con, boolean cleanup) throws Exception {

        HashConfig cstr = new HashConfig();
        WritableResourceNode<?> ctable = cstr.createConfig("table");
        ctable.setProperty("name", tableNameOrg);

        LinkedList<String> pk = new LinkedList<String>();

        for (Field f : fList) {
            ResourceNode<?> cfield = ctable.createConfig("field");
            cfield.setProperty(Dialect.K_NAME, f.createName);
            cfield.setProperty(Dialect.K_TYPE, f.retDbType);
            cfield.setProperty(Dialect.K_SIZE, String.valueOf(f.size));
            cfield.setProperty(Dialect.K_DEFAULT, f.defValue);
            cfield.setProperty(Dialect.K_NOT_NULL, f.nullable ? "no" : "yes");
            cfield.setProperty(Dialect.K_DESCRIPTION, f.description);
            cfield.setProperty(Dialect.K_HINTS, MUri.implodeArray(f.hints));
            LinkedList<String> cat = new LinkedList<String>();
            if (!f.isPersistent()) cat.add(Dialect.C_VIRTUAL);
            if (f.isPrimary) cat.add(Dialect.C_PRIMARY_KEY);
            if (f.getType().isEnum()) cat.add(Dialect.C_ENUMERATION);
            cfield.setProperty(
                    Dialect.K_CATEGORIES, MString.join(cat.iterator(), ",")); // add primary key
            if (f.isPrimary && f.isPersistent()) pk.add(f.createName);
        }

        if (pk.size() > 0) {
            String pkNames = MString.join(pk.iterator(), ",");
            ctable.setProperty(Dialect.K_PRIMARY_KEY, pkNames);
        }

        // create index entries
        for (IndexStruc item : iIdx.values()) {
            ResourceNode<?> cindex = cstr.createConfig("index");
            String n = item.getName();
            if (item.isUnique()) {
                cindex.setString(Dialect.I_TYPE, Dialect.I_UNIQUE);
                cindex.setBoolean(Dialect.I_UNIQUE, true);
            }
            cindex.setString(Dialect.I_HINTS, item.getHints());
            cindex.setString(Dialect.I_NAME, "idx_" + n);
            cindex.setString(Dialect.I_TABLE, tableNameOrg);
            StringBuilder fields = new StringBuilder();
            for (Field field : item) {
                if (fields.length() > 0) fields.append(",");
                fields.append(field.createName);
            }
            cindex.setString(Dialect.I_FIELDS, fields.toString());
        }

        manager.getPool()
                .getDialect()
                .createStructure(cstr, con, manager.getCaoMetadata(), cleanup);
    }

    /**
     * deleteObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param object a {@link java.lang.Object} object.
     * @throws java.lang.Exception if any.
     */
    public void deleteObject(DbConnection con, Object object) throws Exception {

        for (Feature f : features) f.deleteObject(con, object);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        for (Field f : pk) {
            attributes.put(f.name, f.getFromTarget(object));
        }

        schema.internalDeleteObject(con, name, object, attributes);

        sqlDelete.getStatement(con).execute(attributes);
    }

    /**
     * Getter for the field <code>registryName</code>.
     *
     * @return a {@link java.lang.String} object.
     */
    public String getRegistryName() {
        return registryName;
    }

    /**
     * Getter for the field <code>clazz</code>.
     *
     * @return a {@link java.lang.Class} object.
     */
    public Class<?> getClazz() {
        return clazz;
    }

    /**
     * Getter for the field <code>tableName</code>.
     *
     * @return a {@link java.lang.String} object.
     */
    public String getTableName() {
        return tableNameOrg;
    }

    /**
     * getField.
     *
     * @param fName a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.adb.model.Field} object.
     */
    public Field getField(String fName) {
        return fIndex.get(fName);
    }

    /**
     * getPrimaryKeys.
     *
     * @return a {@link java.util.List} object.
     */
    public List<Field> getPrimaryKeys() {
        return pk;
    }

    /**
     * toAttributes.
     *
     * @param pa a {@link de.mhus.lib.annotations.adb.DbPersistent} object.
     * @param pk a {@link de.mhus.lib.annotations.adb.DbPrimaryKey} object.
     * @return a {@link java.lang.String} object.
     */
    public String toAttributes(DbPersistent pa, DbPrimaryKey pk) {

        if (pa == null && pk == null) return "";

        StringBuilder out = new StringBuilder();

        if (pa != null) out.append("size=").append(pa.size());

        if ((pa != null && pa.auto_id()) || (pk != null && pk.auto_id()))
            out.append("&auto_id=true");

        if (pa != null && !pa.nullable()) out.append("&nullable=false");

        if (pa != null) {
            String type = Dialect.typeEnumToString(pa.type());
            if (!MString.isEmpty(type)) out.append("&type=").append(type);

            out.append("&description=").append(MUri.encode(pa.description()));
            if (pa.hints().length > 0)
                out.append("&hints=").append(MUri.encode(MUri.implodeArray(pa.hints())));

            String more = pa.more();
            if (!MString.isEmpty(more)) out.append("&").append(more);
        }
        return out.toString();
    }

    /**
     * getDbRetType.
     *
     * @param ret a {@link java.lang.Class} object.
     * @return a {@link java.lang.String} object.
     */
    protected String getDbRetType(Class<?> ret) {
        String rt = DbType.TYPE.BLOB.name();
        if (ret == int.class) rt = DbType.TYPE.INT.name();
        else if (ret == BigDecimal.class) rt = DbType.TYPE.BIGDECIMAL.name();
        else if (ret == long.class) rt = DbType.TYPE.LONG.name();
        else if (ret == boolean.class) rt = DbType.TYPE.BOOL.name();
        else if (ret == double.class) rt = DbType.TYPE.DOUBLE.name();
        else if (ret == float.class) rt = DbType.TYPE.FLOAT.name();
        else if (ret == String.class) rt = DbType.TYPE.STRING.name();
        else if (ret == Date.class || ret == Calendar.class || ret == java.sql.Date.class)
            rt = DbType.TYPE.DATETIME.name();
        else if (ret == UUID.class) rt = DbType.TYPE.UUID.name();
        else if (ret.isEnum()) rt = DbType.TYPE.INT.name();

        return rt;
    }

    /**
     * Getter for the field <code>name</code>.
     *
     * @return a {@link java.lang.String} object.
     */
    public String getName() {
        return name;
    }

    /**
     * Getter for the field <code>features</code>.
     *
     * @return a {@link java.util.List} object.
     */
    public List<Feature> getFeatures() {
        return features;
    }

    /**
     * Getter for the field <code>attributes</code>.
     *
     * @return a {@link de.mhus.lib.core.directory.ResourceNode} object.
     */
    public ResourceNode<?> getAttributes() {
        return attributes;
    }

    /**
     * getFields.
     *
     * @return an array of {@link de.mhus.lib.adb.model.Field} objects.
     */
    public Field[] getFields() {
        return fList.toArray(new Field[fList.size()]);
    }

    /**
     * getFieldRelations.
     *
     * @return an array of {@link de.mhus.lib.adb.model.FieldRelation} objects.
     */
    public FieldRelation[] getFieldRelations() {
        return relationList.toArray(new FieldRelation[fList.size()]);
    }

    private class IndexStruc extends LinkedList<Field> {

        private static final long serialVersionUID = 1L;
        private String name;
        private TYPE type;
        private String hints;

        public String getHints() {
            return hints;
        }

        public boolean isUnique() {
            if (type == null || type == TYPE.AUTO) return name.startsWith(DbIndex.UNIQUE);
            return type == TYPE.UNIQUE;
        }

        public String getName() {
            return name;
        }
    }
}
