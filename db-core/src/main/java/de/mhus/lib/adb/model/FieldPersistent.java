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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import de.mhus.lib.adb.DbDynamic;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.annotations.adb.DbType;
import de.mhus.lib.core.MDate;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.directory.ResourceNode;
import de.mhus.lib.core.io.MObjectInputStream;
import de.mhus.lib.core.lang.Raw;
import de.mhus.lib.core.pojo.PojoAttribute;
import de.mhus.lib.core.util.MUri;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbResult;

/**
 * FieldPersistent class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class FieldPersistent extends Field {

    private String autoPrefix;

    /**
     * Constructor for FieldPersistent.
     *
     * @param manager a {@link de.mhus.lib.adb.DbManager} object.
     * @param table a {@link de.mhus.lib.adb.model.Table} object.
     * @param isPrimary a boolean.
     * @param readOnly a boolean.
     * @param attribute a {@link de.mhus.lib.core.pojo.PojoAttribute} object.
     * @param attr2 a {@link de.mhus.lib.core.directory.ResourceNode} object.
     * @param dynamicField a {@link de.mhus.lib.adb.DbDynamic.Field} object.
     * @param features an array of {@link java.lang.String} objects.
     * @throws de.mhus.lib.errors.MException if any.
     */
    @SuppressWarnings("unchecked")
    public FieldPersistent(
            DbManager manager,
            Table table,
            boolean isPrimary,
            boolean readOnly,
            PojoAttribute<?> attribute,
            ResourceNode<?> attr2,
            DbDynamic.Field dynamicField,
            String[] features)
            throws MException {
        this.manager = manager;
        this.table = table;
        this.manager = table.manager;
        this.nameOrg = attribute.getName();
        this.name = manager.getPool().getDialect().normalizeColumnName(nameOrg.toLowerCase());
        this.createName = nameOrg.toLowerCase();
        this.methodName = nameOrg;
        this.isPrimary = isPrimary;
        this.attribute = (PojoAttribute<Object>) attribute;
        this.attr = attr2;
        this.dynamicField = dynamicField;
        this.readOnly = readOnly;
        init(features);
    }

    //	public FieldPersistent(DbManager manager, Table table, DbDynamic.Field f) {
    //		this.manager = manager;
    //		this.table = table;
    //		methodName = f.getName();
    //		this.nameOrg = methodName;
    //		this.name = manager.getPool().getDialect().normalizeColumnName(methodName.toLowerCase());
    //		this.createName = methodName.toLowerCase();
    //		this.isPrimary = f.isPrimaryKey();
    //		this.ret = f.getReturnType();
    //		this.attr = f.getAttributes();
    //		this.dynamicField = f;
    //		init();
    //	}

    /** {@inheritDoc} */
    @Override
    protected void init(String[] features) throws MException {
        this.retDbType =
                attr.getExtracted("type", table.getDbRetType(attribute.getType())).toUpperCase();
        //		if (this.retDbType.equals("DATE"))
        //			this.retDbType = "DATETIME";
        this.autoId = attr.getBoolean("auto_id", false);
        this.autoPrefix = attr.getString("auto_prefix", null);
        size = attr.getInt("size", size);
        defValue = attr.getString("default", null);
        nullable = attr.getBoolean("nullable", true);
        description = attr.getExtracted("description");
        hints = MUri.explodeArray(attr.getString("hints"));
        if (isPrimary) nullable = false;

        super.init(features);
    }

    /** {@inheritDoc} */
    @Override
    public void fillNameMapping(HashMap<String, Object> nameMapping) {
        nameMapping.put(
                "db." + manager.getMappingName(table.clazz) + "." + methodName, new Raw(name));
        if (attribute.getType().isEnum()) {
            int cnt = 0;
            for (Object c : attribute.getType().getEnumConstants()) {
                nameMapping.put(
                        "db." + manager.getMappingName(table.clazz) + "." + methodName + "." + c,
                        cnt);
                cnt++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void prepareCreate(Object obj) throws Exception {
        if (autoId) {
            if (attribute.getType() == UUID.class) {
                Object curVal = get(obj);
                if (curVal == null) {
                    UUID uuid = UUID.randomUUID();
                    if (MString.isSet(autoPrefix)) {
                        String uidStr = uuid.toString();
                        uidStr = autoPrefix + uidStr.substring(autoPrefix.length());
                        uuid = UUID.fromString(uidStr);
                    }
                    set(obj, uuid);
                }
            } else if (attribute.getType() == String.class) {
                Object curVal = get(obj);
                if (curVal == null) {
                    UUID uuid = UUID.randomUUID();
                    String uidStr = uuid.toString();
                    if (MString.isSet(autoPrefix)) {
                        uidStr = autoPrefix + uidStr.substring(autoPrefix.length());
                    }
                    set(obj, uidStr);
                }
            } else if (attribute.getType() == long.class || attribute.getType() == int.class) {
                manager.getSchema().doCreateUniqueIdFor(table, this, obj, name, manager);
            } else log().w("can't set auto_id to object", name);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object getFromTarget(Object obj) throws Exception {
        Object out = get(obj);
        if (retDbType.equals(DbType.TYPE.BLOB.name())) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(os);
            oos.writeObject(out);
            ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
            return is;
        }
        return out;
    }

    /** {@inheritDoc} */
    @Override
    public void setToTarget(DbResult res, Object obj) throws Exception {

        if (retDbType.equals(DbType.TYPE.INT.name())) set(obj, res.getInt(name));
        else if (retDbType.equals(DbType.TYPE.LONG.name())) set(obj, res.getLong(name));
        else if (retDbType.equals(DbType.TYPE.BOOL.name())) set(obj, res.getBoolean(name));
        else if (retDbType.equals(DbType.TYPE.DATETIME.name())) {
            try {
                if (attribute.getType() == Date.class) set(obj, res.getTimestamp(name));
                else if (attribute.getType() == java.sql.Date.class) {
                    Timestamp time = res.getTimestamp(name);
                    set(obj, time == null ? null : new java.sql.Date(time.getTime()));
                } else set(obj, new MDate(res.getTimestamp(name)).toCalendar());
            } catch (java.sql.SQLException sqle) {
                // Caused by: java.sql.SQLException: Value '0000-00-00 00:00:00' can not be
                // represented as java.sql.Timestamp
                set(obj, null);
            }
        } else if (retDbType.equals(DbType.TYPE.DOUBLE.name())) set(obj, res.getDouble(name));
        else if (retDbType.equals(DbType.TYPE.BIGDECIMAL.name())) set(obj, res.getBigDecimal(name));
        else if (retDbType.equals(DbType.TYPE.FLOAT.name())) set(obj, res.getFloat(name));
        else if (retDbType.equals(DbType.TYPE.STRING.name())) set(obj, res.getString(name));
        else if (retDbType.equals(DbType.TYPE.UUID.name())) {
            String o = res.getString(name);
            if (o == null) set(obj, (UUID) null);
            else
                try {
                    set(obj, UUID.fromString(o));
                } catch (Throwable t) {
                    log().d("uuid", name, o, t);
                    set(obj, (UUID) null);
                }
        } else if (retDbType.equals(DbType.TYPE.BLOB.name())) {
            InputStream st = res.getBinaryStream(name);
            if (st != null) {
                @SuppressWarnings("resource")
                MObjectInputStream ois = new MObjectInputStream(st);
                ois.setActivator(manager.getActivator());
                //												ois.setClassLoader(manager.getActivator());

                Object o = ois.readObject();
                set(obj, o);
            } else set(obj, null);
        } else log().d("can't set to target ", name, retDbType);
    }

    /** {@inheritDoc} */
    @Override
    public boolean changed(DbResult res, Object obj) throws Exception {

        if (retDbType.equals(DbType.TYPE.INT.name())) return different(obj, res.getInt(name));
        else if (retDbType.equals(DbType.TYPE.LONG.name()))
            return different(obj, res.getLong(name));
        else if (retDbType.equals(DbType.TYPE.BOOL.name()))
            return different(obj, res.getBoolean(name));
        else if (retDbType.equals(DbType.TYPE.DATETIME.name())) {
            if (attribute.getType() == Date.class) return different(obj, res.getTimestamp(name));
            else return different(obj, new MDate(res.getTimestamp(name)).toCalendar());
        } else if (retDbType.equals(DbType.TYPE.DOUBLE.name()))
            return different(obj, res.getDouble(name));
        else if (retDbType.equals(DbType.TYPE.FLOAT.name()))
            return different(obj, res.getFloat(name));
        else if (retDbType.equals(DbType.TYPE.STRING.name()))
            return different(obj, res.getString(name));
        else if (retDbType.equals(DbType.TYPE.UUID.name())) {
            String o = res.getString(name);
            if (o == null) return different(obj, (UUID) null);
            else
                try {
                    return different(obj, UUID.fromString(o));
                } catch (Throwable t) {
                    log().d("uuid", name, o, t);
                    return different(obj, (UUID) null);
                }
        } else if (retDbType.equals(DbType.TYPE.BLOB.name())) {
            InputStream st = res.getBinaryStream(name);
            if (st != null) {
                @SuppressWarnings("resource")
                MObjectInputStream ois = new MObjectInputStream(st);
                ois.setClassLoader(manager.getActivator());
                Object o = ois.readObject();
                return different(obj, o);
            } else return different(obj, null);
        } else if (retDbType.equals(DbType.TYPE.BIGDECIMAL.name())) {
            return different(obj, res.getBigDecimal(name));
        } else log().d("can't test", name, retDbType);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isPersistent() {
        return true;
    }
}
