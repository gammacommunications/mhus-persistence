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
package de.mhus.lib.adb;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import de.mhus.lib.adb.model.AttributeFeature;
import de.mhus.lib.adb.model.AttributeFeatureCut;
import de.mhus.lib.adb.model.Feature;
import de.mhus.lib.adb.model.FeatureAccessManager;
import de.mhus.lib.adb.model.FeatureCut;
import de.mhus.lib.adb.model.Field;
import de.mhus.lib.adb.model.FieldPersistent;
import de.mhus.lib.adb.model.FieldVirtual;
import de.mhus.lib.adb.model.Table;
import de.mhus.lib.adb.model.TableAnnotations;
import de.mhus.lib.adb.model.TableDynamic;
import de.mhus.lib.adb.transaction.LockStrategy;
import de.mhus.lib.adb.util.AdbUtil;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.directory.ResourceNode;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.core.pojo.PojoAttribute;
import de.mhus.lib.core.pojo.PojoModel;
import de.mhus.lib.core.pojo.PojoModelFactory;
import de.mhus.lib.errors.AccessDeniedException;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DbResult;

/**
 * Define the schema with a new instance of this class. It can handle and manipulate all activities.
 * It's also a factory for the loaded objects.
 *
 * @author mikehummel
 */
public abstract class DbSchema extends MObject implements PojoModelFactory {

    protected String tablePrefix = "";
    private LinkedList<Class<? extends Persistable>> objectTypes;
    protected LockStrategy lockStrategy; // set this object to enable locking

    public abstract void findObjectTypes(List<Class<? extends Persistable>> list);

    @SuppressWarnings("unchecked")
    public final Class<? extends Persistable>[] getObjectTypes() {
        initObjectTypes();
        return (Class<? extends Persistable>[])
                objectTypes.toArray(new Class<?>[objectTypes.size()]);
    }

    void resetObjectTypes() {
        objectTypes = null;
    }

    @Override
    public PojoModel createPojoModel(Class<?> clazz) {
        return AdbUtil.createDefaultPojoModel(clazz);
    }

    /**
     * This should be called after the manager is created.
     *
     * @param manager
     */
    public void doPostInit(DbManager manager) {}

    /**
     * Overwrite this method to get the configuration object and initialize the schem. It should be
     * called by the creator to initialize the schema before it is given to the manager.
     *
     * @param config
     */
    public void doInit(ResourceNode<?> config) {}

    /**
     * Masquerade the table names if needed. By default a tablePrefix is set for the table.
     *
     * @param name
     * @return x
     */
    public String getTableName(String name) {
        return tablePrefix + name;
    }

    /**
     * Object factory to create different kinds of objects for one table.
     *
     * @param clazz
     * @param registryName
     * @param ret could be null, return the default object
     * @param manager
     * @param isPersistent
     * @return x x
     * @throws Exception
     */
    public Object createObject(
            Class<?> clazz,
            String registryName,
            DbResult ret,
            DbManager manager,
            boolean isPersistent)
            throws Exception {
        Object object = manager.getActivator().createObject(clazz.getCanonicalName());
        if (object instanceof DbObject) {
            ((DbObject) object).doInit(manager, registryName, isPersistent);
        }
        return object;
    }

    /**
     * If no registryName is set in the manager this will ask the schema for the correct
     * registryName.
     *
     * @param object
     * @param manager
     * @return x
     */
    public Class<? extends Persistable> findClassForObject(Object object, DbManager manager) {
        initObjectTypes();
        if (object instanceof Class<?>) {
            for (Class<? extends Persistable> c : objectTypes)
                if (((Class<?>) object).isAssignableFrom(c)) return c;
        }

        for (Class<? extends Persistable> c : objectTypes) if (c.isInstance(object)) return c;
        return null;
    }

    protected synchronized void initObjectTypes() {
        if (objectTypes != null) return;
        objectTypes = new LinkedList<>();
        findObjectTypes(objectTypes);
    }

    /**
     * Return a new unique Id for a new entry in the table. Only used for auto_id fields with type
     * long. The default implementation is not save !!!
     *
     * @param table
     * @param field
     * @param obj
     * @param name
     * @param manager
     */
    public void doCreateUniqueIdFor(
            Table table, Field field, Object obj, String name, DbManager manager) {

        // Ask Object-Class/Object to create an unique Id
        try {
            Method helperMethod =
                    field.getType()
                            .getMethod(
                                    "doCreateUniqueIdFor_" + field.getName(),
                                    new Class[] {DbManager.class});
            Object res = helperMethod.invoke(obj, new Object[] {manager});
            if (res == null) return;
            field.set(obj, res);
            return;
        } catch (NoSuchMethodException nsme) {
            log().t(field, nsme);
        } catch (Throwable t) {
            log().t(field, t);
            return;
        }

        //		long id = base(UniqueId.class).nextUniqueId();
        //		field.set(obj , id);
    }

    /**
     * Overwrite this to get the hook in the schema. By default it's delegated to the object.
     * Remember to call the super.
     *
     * @param table
     * @param object
     * @param con
     * @param manager
     */
    public void doPreCreate(Table table, Object object, DbConnection con, DbManager manager) {
        if (object instanceof DbObject) {
            ((DbObject) object)
                    .doInit(
                            manager,
                            table.getRegistryName(),
                            ((DbObject) object).isAdbPersistent());
            ((DbObject) object).doPreCreate(con);
        }
    }

    /**
     * Overwrite this to get the hook in the schema. By default it's delegated to the object.
     * Remember to call the super.
     *
     * @param table
     * @param object
     * @param con
     * @param manager
     */
    public void doPreSave(Table table, Object object, DbConnection con, DbManager manager) {
        if (object instanceof DbObject) {
            ((DbObject) object).doPreSave(con);
        }
    }

    /**
     * Return true if you want to store persistent information about the schema in the database. Use
     * Manager.getSchemaProperties() to access the properties. Default value is true.
     *
     * @return x
     */
    public boolean hasPersistentInfo() {
        return true;
    }

    /**
     * Return the name of the schema used for example for the schema property table. Default is the
     * simple name of the class.
     *
     * @return x
     */
    public String getSchemaName() {
        return getClass().getSimpleName();
    }

    /**
     * Overwrite this if you want to provide default query attributes by default. Name mapping will
     * provide all table and field names for the used db activities.
     *
     * @param nameMapping
     */
    public void doFillNameMapping(HashMap<String, Object> nameMapping) {}

    /**
     * Overwrite this to get the hook in the schema. By default it's delegated to the object.
     * Remember to call the super.
     *
     * @param table
     * @param object
     * @param con
     * @param dbManager
     */
    public void doPreDelete(
            Table table, Persistable object, DbConnection con, DbManager dbManager) {
        if (object instanceof DbObject) {
            ((DbObject) object).doPreDelete(con);
        }
    }

    /**
     * Overwrite this to get the hook in the schema. By default it's delegated to the object.
     * Remember to call the super.
     *
     * @param table
     * @param object
     * @param con
     * @param manager
     */
    public void doPostLoad(Table table, Persistable object, DbConnection con, DbManager manager) {
        if (object instanceof DbObject) {
            ((DbObject) object).doPostLoad(con);
        }
    }

    public void doPostCreate(Table table, Persistable object, DbConnection con, DbManager manager) {
        if (object instanceof DbObject) {
            ((DbObject) object).doPostCreate(con);
        }
    }
    /**
     * Overwrite this to get the hook in the schema. By default it's delegated to the object.
     * Remember to call the super.
     *
     * @param c
     * @param object
     * @param con
     * @param dbManager
     */
    public void doPostDelete(Table c, Persistable object, DbConnection con, DbManager dbManager) {
        if (object instanceof DbObject) {
            ((DbObject) object).doPostDelete(con);
        }
    }

    /**
     * Called if the schema property table is created. This allows the schema to add the default
     * schema values to the properties set.
     *
     * @param dbManager
     */
    public void doInitProperties(DbManager dbManager) {}

    /**
     * Overwrite this to validate the current database version and maybe migrate to a newer version.
     * This only works if schema property is enabled. TODO Extend the default functionality to
     * manage the versions.
     *
     * @param dbManager
     * @param currentVersion
     * @throws MException
     */
    public void doMigrate(DbManager dbManager, long currentVersion) throws MException {}

    /**
     * If you provide access management return an access manager instance for the given table. This
     * will most time be called one at initialization time.
     *
     * @param c
     * @return x The manager or null
     */
    public DbAccessManager getAccessManager(Table c) {
        return null;
    }

    @Override
    public String toString() {
        initObjectTypes();
        return MSystem.toString(this, getSchemaName(), objectTypes);
    }

    public Table createTable(
            DbManager manager,
            Class<? extends Persistable> clazz,
            String registryName,
            String tableName) {

        boolean isDynamic = true;
        try {
            clazz.asSubclass(DbDynamic.class);
        } catch (ClassCastException e) {
            isDynamic = false;
        }

        Table table = null;
        if (isDynamic) table = new TableDynamic();
        else table = new TableAnnotations();
        table.init(manager, clazz, registryName, tableName);
        return table;
    }

    public Feature createFeature(DbManager manager, Table table, String name) {

        try {
            Feature feature = null;

            name = name.trim().toLowerCase();

            if (name.equals("accesscontrol")) feature = new FeatureAccessManager();
            else if (name.equals(FeatureCut.NAME)) feature = new FeatureCut();

            if (feature != null) feature.init(manager, table);
            else log().w("feature not found", name);
            return feature;
        } catch (Exception e) {
            log().t("feature", name, e);
            return null;
        }
    }

    public AttributeFeature createAttributeFeature(DbManager manager, Field field, String name) {

        try {
            AttributeFeature feature = null;

            if (name.equals(AttributeFeatureCut.NAME)) {
                feature = new AttributeFeatureCut();
            }

            if (feature != null) feature.init(manager, field);
            else log().w("attribute feature not found", name);

            return feature;
        } catch (Exception e) {
            log().t("feature", name, e);
            return null;
        }
    }

    public Field createField(
            DbManager manager,
            Table table,
            boolean pk,
            boolean readOnly,
            boolean virtual,
            PojoAttribute<?> attribute,
            ResourceNode<?> attr,
            DbDynamic.Field dynamicField,
            String[] features)
            throws MException {

        Field field = null;
        if (virtual) field = new FieldVirtual(table, pk, attribute, attr, features);
        else
            field =
                    new FieldPersistent(
                            manager, table, pk, readOnly, attribute, attr, dynamicField, features);

        return field;
    }

    public void internalCreateObject(
            DbConnection con, String name, Object object, HashMap<String, Object> attributes) {}

    public void internalSaveObject(
            DbConnection con, String name, Object object, HashMap<String, Object> attributes) {}

    public void internalDeleteObject(
            DbConnection con, String name, Object object, HashMap<String, Object> attributes) {}

    public void onFillObjectException(Table table, Object obj, DbResult res, Field f, Throwable t)
            throws Throwable {
        throw t;
    }

    /**
     * Return a default connection if no connection is given for the operation with the object. If
     * you want to work with transactions use this method to return a transaction bound connection.
     * By default a new connection from the pool are used. You may overwrite the commit() or
     * rollback() methods.
     *
     * @param pool
     * @return x
     * @throws Exception
     */
    public DbConnection getConnection(DbPool pool) throws Exception {
        DbConnection con = (DbConnection) DbTransaction.getConnection(pool);
        if (con != null) return con;
        return pool.getConnection();
    }

    /**
     * Close the default connection given with getConnection().
     *
     * @param pool
     * @param con
     */
    public void closeConnection(DbPool pool, DbConnection con) {
        DbConnection c = (DbConnection) DbTransaction.getConnection(pool);
        if (c != null) return;
        con.close();
    }

    /**
     * Used to commit a default connection. See getConnection()
     *
     * @param pool
     * @param con
     * @throws Exception
     */
    public void commitConnection(DbPool pool, DbConnection con) throws Exception {
        DbConnection c = (DbConnection) DbTransaction.getConnection(pool);
        if (c != null) return;
        con.commit();
    }

    public LockStrategy getLockStrategy() {
        return lockStrategy;
    }

    public void authorizeSaveForceAllowed(DbConnection con, Table table, Object object, boolean raw)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    public void authorizeUpdateAttributes(
            DbConnection con, Table table, Object object, boolean raw, String... attributeNames)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    public void injectObject(Object object, DbManager manager, Table table) {
        if (object instanceof DbObject) ((DbObject) object).setDbHandler(manager);
        table.injectObject(object);
    }

    public void authorizeReadAttributes(
            DbConnection con,
            DbManager dbManagerJdbc,
            Class<?> clazz,
            String registryName,
            String attribute) {
        throw new AccessDeniedException();
    }
}
