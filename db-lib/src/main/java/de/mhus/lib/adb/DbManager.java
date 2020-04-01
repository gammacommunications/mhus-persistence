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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.mhus.lib.adb.model.Field;
import de.mhus.lib.adb.model.Table;
import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.adb.util.AdbUtil;
import de.mhus.lib.adb.util.DbProperties;
import de.mhus.lib.annotations.jmx.JmxManaged;
import de.mhus.lib.cao.util.MetadataBundle;
import de.mhus.lib.core.MActivator;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.jmx.MJmx;
import de.mhus.lib.core.pojo.PojoModelFactory;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.NotFoundException;
import de.mhus.lib.sql.DbConnection;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DbResult;
import de.mhus.lib.xdb.XdbService;
import de.mhus.lib.xdb.XdbType;

/**
 * The implementation hold the table definitions and handle all operations on the objects. It's the
 * central point of access.
 */
@JmxManaged(descrition = "ADB manager information interface")
public abstract class DbManager extends MJmx implements DbObjectHandler, XdbService {

    @Override
    public abstract <T> T getObjectByQualification(AQuery<T> qualification) throws MException;

    public abstract <T> DbCollection<T> getByQualification(
            Class<T> clazz, String qualification, Map<String, Object> attributes) throws MException;

    public abstract <T> DbCollection<T> getByQualification(
            T object, String qualification, Map<String, Object> attributes) throws MException;

    @Override
    public abstract <T> DbCollection<T> getByQualification(AQuery<T> qualification)
            throws MException;

    /**
     * Get an collection of objects by it's qualification. The qualification is the WHERE part of a
     * query. e.g. "$db.table.name$ like 'Joe %'"
     *
     * @param <T> Type of the object
     * @param con The connection or null
     * @param object a representation of the object (empty object) or a object to recycle in the
     *     collection.
     * @param registryName The name of the registry (if not default) or null
     * @param qualification The WHERE string
     * @param attributes attributes or null if not needed
     * @return A collection with the results
     * @throws MException
     */
    public abstract <T> DbCollection<T> getByQualification(
            DbConnection con,
            T object,
            String registryName,
            String qualification,
            Map<String, Object> attributes)
            throws MException;

    public abstract String createSqlSelect(Class<?> clazz, String columns, String qualification);

    public abstract <T> long getCountAll(Class<T> clazz) throws MException;

    public abstract <T> long getCountByQualification(
            Class<T> clazz, String qualification, Map<String, Object> attributes) throws MException;

    public abstract <T> long getCountByQualification(
            T object, String qualification, Map<String, Object> attributes) throws MException;

    public abstract <T> long getCountByQualification(AQuery<T> qualification) throws MException;

    /**
     * Returns the count of all found objects for the qualification. It's faster than loading all
     * data from the database with getByQualification.
     *
     * @param con
     * @param object
     * @param registryName
     * @param qualification
     * @param attributes
     * @return x
     * @throws MException
     */
    public abstract <T> long getCountByQualification(
            DbConnection con,
            T object,
            String registryName,
            String qualification,
            Map<String, Object> attributes)
            throws MException;

    public abstract <T> long getMax(Class<T> clazz, String field) throws MException;

    public abstract <T> long getMaxByQualification(
            Class<T> clazz, String field, String qualification, Map<String, Object> attributes)
            throws MException;

    public abstract <T> long getMaxByQualification(
            T object, String field, String qualification, Map<String, Object> attributes)
            throws MException;

    public abstract <T> long getMaxByQualification(String field, AQuery<T> qualification)
            throws MException;

    public abstract <T> long getMaxByQualification(
            DbConnection con,
            T object,
            String registryName,
            String field,
            String qualification,
            Map<String, Object> attributes)
            throws MException;

    public abstract <T> DbCollection<T> executeQuery(
            T clazz, String query, Map<String, Object> attributes) throws MException;

    public abstract <T, R> List<R> getAttributeByQualification(
            Class<? extends T> clazz,
            String field,
            String qualification,
            Map<String, Object> attributes)
            throws MException;

    public abstract <T, R> List<R> getAttributeByQualification(
            String field, AQuery<? extends T> qualification) throws MException;

    public abstract <T, R> List<R> getAttributeByQualification(
            DbConnection con,
            Class<? extends T> clazz,
            String registryName,
            String field,
            String qualification,
            Map<String, Object> attributes)
            throws MException;

    /**
     * Returns an collection.
     *
     * @param <T>
     * @param con DbConnection or null
     * @param clazz Empty Object class
     * @param registryName registry name or null
     * @param query The query, remember to return all attributes
     * @param attributes attributes for the query or null
     * @return a collection with the results
     * @throws MException
     */
    public abstract <T> DbCollection<T> executeQuery(
            DbConnection con,
            T clazz,
            String registryName,
            String query,
            Map<String, Object> attributes)
            throws MException;

    /**
     * Returns a long value out of a query.
     *
     * @param con
     * @param attributeName
     * @param query
     * @param attributes
     * @return x
     * @throws MException
     */
    public abstract <T> long executeCountQuery(
            DbConnection con, String attributeName, String query, Map<String, Object> attributes)
            throws MException;

    /**
     * Returns the persistent schema properties if supported.
     *
     * @return The properties or null
     */
    @JmxManaged(descrition = "Database Properties of the Schema")
    public abstract DbProperties getSchemaProperties();

    public abstract Object getObject(String registryName, Object... keys) throws MException;

    @Override
    public abstract <T> T getObject(Class<T> clazz, Object... keys) throws MException;

    public abstract <T> T getObject(DbConnection con, Class<T> clazz, Object... keys)
            throws MException;

    /**
     * Return an object from the database defined by the primary key - like a load operation. If
     * more the one attributes is needed, the order is alphabetic by the attribute name.
     *
     * @param con A connection to use or null
     * @param registryName The registry name
     * @param keys The primary key values
     * @return x
     * @throws MException
     */
    public abstract Object getObject(DbConnection con, String registryName, Object... keys)
            throws MException;

    //

    public abstract boolean existsObject(String registryName, Object... keys) throws MException;

    abstract void fillObject(String registryName, Object object, DbConnection con, DbResult res)
            throws MException;

    public abstract <T> boolean existsObject(Class<T> clazz, Object... keys) throws MException;

    public abstract <T> boolean existsObject(DbConnection con, Class<T> clazz, Object... keys)
            throws MException;

    /**
     * Return an object from the database defined by the primary key - like a load operation. If
     * more the one attributes is needed, the order is alphabetic by the attribute name.
     *
     * @param con A connection to use or null
     * @param registryName The registry name
     * @param keys The primary key values
     * @return x
     * @throws MException
     */
    public abstract boolean existsObject(DbConnection con, String registryName, Object... keys)
            throws MException;

    public abstract void reloadObject(String registryName, Object object) throws MException;

    /**
     * Reload the object data from database.
     *
     * @param con The connection to use or null
     * @param registryName The registry name or null
     * @param object The object to fill
     * @throws MException
     */
    @Override
    public abstract void reloadObject(DbConnection con, String registryName, Object object)
            throws MException;

    @Override
    public abstract boolean objectChanged(Object object) throws MException;

    public abstract boolean objectChanged(String registryName, Object object) throws MException;

    /**
     * Check the object date against the data in the database. If the data differs it will return
     * true.
     *
     * @param con
     * @param registryName
     * @param object
     * @return x
     * @throws MException
     */
    public abstract boolean objectChanged(DbConnection con, String registryName, Object object)
            throws MException;

    /**
     * Fill a object with the values of an entry. This can cause big problems and exceptions if the
     * kind of the object is not correct. This is used to recycle objects. It's not save in every
     * case.
     *
     * @param con The connection to use or null
     * @param registryName The rgistryName or null
     * @param object The object to fill
     * @param keys The primary keys
     * @throws MException
     */
    public abstract void fillObject(
            DbConnection con, String registryName, Object object, Object... keys) throws MException;

    public abstract void createObject(Object object) throws MException;

    public abstract void createObject(String registryName, Object object) throws MException;

    @Override
    public abstract void createObject(DbConnection con, Object object) throws MException;

    /**
     * Create a object in the database.
     *
     * @param con The connection to use or null.
     * @param registryName The registryName or null if it is a simple object reference
     * @param object The object to create.
     * @throws MException
     */
    public abstract void createObject(DbConnection con, String registryName, Object object)
            throws MException;

    public abstract void saveObject(Object object) throws MException;

    public abstract void saveObject(String registryName, Object object) throws MException;

    public abstract void saveObject(DbConnection con, Object object) throws MException;

    /**
     * Update the data of the object in the database. This will not create an object.
     *
     * @param con The connection to use or null
     * @param registryName The registryName or null
     * @param object The object to save
     * @throws MException
     */
    @Override
    public abstract void saveObject(DbConnection con, String registryName, Object object)
            throws MException;

    public abstract void saveObjectForce(Object object, boolean raw) throws MException;

    public abstract void saveObjectForce(String registryName, Object object, boolean raw)
            throws MException;

    public abstract void saveObjectForce(DbConnection con, Object object, boolean raw)
            throws MException;

    /**
     * Update the data of the object in the database. This will not create an object. This update
     * will also update read-only fields.
     *
     * @param con The connection to use or null
     * @param registryName The registryName or null
     * @param object The object to save
     * @param raw If true no events like preSave are called
     * @throws MException
     */
    public abstract void saveObjectForce(
            DbConnection con, String registryName, Object object, boolean raw) throws MException;

    public abstract void updateAttributes(Object object, boolean raw, String... attributeNames)
            throws MException;

    public abstract void updateAttributes(
            String registryName, Object object, boolean raw, String... attributeNames)
            throws MException;

    public abstract void updateAttributes(
            DbConnection con, Object object, boolean raw, String... attributeNames)
            throws MException;

    public abstract void updateAttributes(
            DbConnection con,
            String registryName,
            Object object,
            boolean raw,
            String... attributeNames)
            throws MException;

    public abstract void deleteObject(Object object) throws MException;

    public abstract void deleteObject(String registryName, Object object) throws MException;

    public abstract void deleteObject(DbConnection con, Object object) throws MException;

    /**
     * Delete an object in the database.
     *
     * @param con The connection to use or null
     * @param registryName The registry name to use or null
     * @param object The object to delete from database
     * @throws MException
     */
    @Override
    public abstract void deleteObject(DbConnection con, String registryName, Object object)
            throws MException;

    @Override
    public abstract boolean isConnected();

    @Override
    public abstract void connect() throws MException;

    public abstract void disconnect();

    public abstract void reconnect() throws MException;

    @JmxManaged(descrition = "Used Schema")
    public abstract DbSchema getSchema();

    public abstract DbPool getPool();

    public abstract MActivator getActivator();

    @JmxManaged(descrition = "Current mapping of the table and column names")
    public abstract Map<String, Object> getNameMapping();

    public abstract MetadataBundle getCaoMetadata();

    @JmxManaged(descrition = "Returns valide registry names")
    public abstract String[] getRegistryNames();

    @JmxManaged(descrition = "Returns the table for the registry name")
    public abstract Table getTable(String registryName);

    public abstract Object createSchemaObject(String registryName) throws Exception;

    public abstract String getRegistryName(Object object);

    public abstract String getMappingName(Class<?> clazz);

    @Override
    public abstract <T extends Persistable> T inject(T object);

    @Override
    public abstract <T extends Persistable> DbCollection<T> getAll(Class<T> clazz)
            throws MException;

    public abstract <T> String toQualification(AQuery<T> qualification);

    @Override
    public <T> XdbType<T> getType(Class<T> type) throws NotFoundException {
        String tableName;
        try {
            tableName = AdbUtil.getTableName(this, type);
        } catch (IOException e) {
            throw new NotFoundException("Table not found", type, e);
        }
        Table table = this.getTable(tableName);
        if (table == null) throw new NotFoundException("Table not found", type);
        return new Type<T>(this, table);
    }

    @Override
    public <T> XdbType<T> getType(String name) throws NotFoundException {
        String tableName;
        try {
            tableName = AdbUtil.getTableName(this, name);
        } catch (IOException e) {
            throw new NotFoundException("Table not found", name, e);
        }
        Table table = this.getTable(tableName);
        if (table == null) throw new NotFoundException("Table not found", name);
        return new Type<T>(this, table);
    }

    @Override
    public List<String> getTypeNames() {
        LinkedList<String> out = new LinkedList<>();
        for (Class<? extends Persistable> o : getSchema().getObjectTypes())
            out.add(o.getSimpleName());
        return out;
    }

    @Override
    public void updateSchema(boolean cleanup) throws MException {
        reconnect();
    }

    @Override
    public String getSchemaName() {
        return getSchema().getSchemaName();
    }

    @Override
    public PojoModelFactory getPojoModelFactory() {
        return getSchema();
    }

    @Override
    public void delete(Persistable object) throws MException {
        deleteObject(object);
    }

    @Override
    public void save(Persistable object) throws MException {
        saveObject(object);
    }

    private static class Type<T> implements XdbType<T> {

        private Table table;
        private DbManager service;

        public Type(DbManager service, Table table) {
            this.service = service;
            this.table = table;
        }

        @SuppressWarnings("unchecked")
        @Override
        public DbCollection<T> getByQualification(
                String search, Map<String, Object> parameterValues) throws MException {
            return (DbCollection<T>)
                    service.getByQualification(table.getClazz(), search, parameterValues);
        }

        @Override
        public DbCollection<T> getByQualification(AQuery<T> query) throws MException {
            return (DbCollection<T>) service.getByQualification(query);
        }

        @Override
        public List<String> getAttributeNames() {
            LinkedList<String> out = new LinkedList<>();
            for (Field f : table.getFields()) out.add(f.getName());
            return out;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <F> F prepareManualValue(String name, Object value) {
            return (F) AdbUtil.createAttribute(table.getField(name).getType(), value);
        }

        @Override
        public void set(Object object, String name, Object v) throws MException {
            try {
                table.getField(name).set(object, v);
            } catch (Throwable t) {
                throw new MException(name, t);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <F> F get(Object object, String name) throws MException {
            try {
                return (F) table.getField(name).get(object);
            } catch (Throwable t) {
                throw new MException(name, t);
            }
        }

        @Override
        public void createObject(Object object) throws MException {
            service.createObject(object);
        }

        @Override
        public String getIdAsString(Object object) throws Exception {
            StringBuilder out = new StringBuilder();
            for (Field f : table.getPrimaryKeys()) {
                if (out.length() > 0) out.append(",");
                out.append(f.get(object));
            }

            return out.toString();
        }

        @Override
        public long count(String search, Map<String, Object> parameterValues) throws MException {
            return service.getCountByQualification(table.getClazz(), search, parameterValues);
        }

        @Override
        public long count(AQuery<T> query) throws MException {
            return service.getCountByQualification(query);
        }

        @Override
        public T newInstance() throws Exception {
            @SuppressWarnings("unchecked")
            T out = (T) table.getClazz().getDeclaredConstructor().newInstance();
            service.inject((Persistable) out);
            return out;
        }

        @Override
        public void deleteObject(Object object) throws MException {
            service.deleteObject(object);
        }

        @Override
        public Class<?> getAttributeType(String name) {
            return table.getField(name).getType();
        }

        @Override
        public boolean isPrimaryKey(String name) {
            for (Field f : table.getPrimaryKeys()) if (f.getName().equals(name)) return true;
            return false;
        }

        @Override
        public boolean isPersistent(String name) {
            return table.getField(name).isPersistent();
        }

        @Override
        public String getTechnicalName(String name) {
            return table.getField(name).getMappedName();
        }

        @Override
        public void saveObjectForce(Object object, boolean raw) throws MException {
            service.saveObjectForce(object, raw);
        }

        @Override
        public void saveObject(Object object) throws MException {
            service.saveObject(object);
        }

        @Override
        public Object getId(Object object) throws MException {
            List<Field> pk = table.getPrimaryKeys();
            try {
                if (pk.size() < 1) return null;
                if (pk.size() == 1) return pk.get(0).get(object);

                Object[] out = new Object[pk.size()];
                int cnt = 0;
                for (Field f : pk) {
                    out[cnt] = f.get(object);
                    cnt++;
                }

                return out;
            } catch (Throwable t) {
                throw new MException(pk, t);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public T getObject(String... keys) throws MException {
            return (T) service.getObject(table.getClazz(), (Object[]) keys);
        }

        @SuppressWarnings("unchecked")
        @Override
        public DbCollection<T> getAll() throws MException {
            return (DbCollection<T>) service.getAll((Class<Persistable>) table.getClazz());
        }

        @Override
        public String toString() {
            return MSystem.toString(this, table == null ? "?" : table.getName());
        }
    }
}
