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
package de.mhus.lib.xdb;

import java.util.List;
import java.util.Map;

import de.mhus.lib.adb.DbCollection;
import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.errors.MException;

public interface XdbType<T> {

    /**
     * Search and return objects from db
     *
     * @param query
     * @param parameterValues A list of parameter values or null if not needed
     * @return a result stream. You need to close the result or iterate it until the end (automatic
     *     close)
     * @throws MException
     */
    DbCollection<T> getByQualification(String query, Map<String, Object> parameterValues)
            throws MException;

    /**
     * Search and return objects from db
     *
     * @param query Adb.query()....
     * @return a result stream. You need to close the result or iterate it until the end (automatic
     *     close)
     * @throws MException
     */
    DbCollection<T> getByQualification(AQuery<T> query) throws MException;

    /**
     * Search and return objects from db
     *
     * @param query Adb.query()....
     * @return a the first found object by the query or null if no object was found.
     * @throws MException
     */
    default T getObjectByQualification(AQuery<T> query) throws MException {
        DbCollection<T> col = getByQualification(query);
        try {
            if (!col.hasNext()) return null;
            return col.next();
        } finally {
            col.close();
        }
    }

    /**
     * List all known attribute names
     *
     * @return All known attribute names.
     */
    List<String> getAttributeNames();

    /**
     * Prepare a value to be stored in a object attribute. Use case is a manually inserted value by
     * the user.
     *
     * @param attributeName The name of the attribute
     * @param value The initial value or null (null or '[null]' will return null)
     * @return A prepared value of this attribute using the initial value
     */
    <F> F prepareManualValue(String attributeName, Object value);

    /**
     * Set the value of an attribute to the object.
     *
     * @param object
     * @param name
     * @param value
     * @throws MException
     */
    void set(Object object, String name, Object value) throws MException;

    /**
     * Load/get the value of an attribute of the object.
     *
     * @param object
     * @param name
     * @return the value
     * @throws MException
     */
    <F> F get(Object object, String name) throws MException;

    /**
     * Create a new object. If the object was already persistent a new clone will be created.
     *
     * @param object
     * @throws Exception
     */
    void createObject(Object object) throws Exception;

    /**
     * Returns the id of the object as string representation.
     *
     * @param object
     * @return A string representation of the primary key or id
     * @throws Exception
     */
    String getIdAsString(Object object) throws Exception;

    /**
     * Returns the id of the object as object, could also be an array if a combined primary key is
     * used.
     *
     * @param object
     * @return The primary key or id. If it's a combined primary key a object array will be
     *     returned.
     * @throws MException
     */
    Object getId(Object object) throws MException;

    /**
     * Return the amount of objects in the database.
     *
     * @param query
     * @param parameterValues
     * @return count the objects by query
     * @throws MException
     */
    long count(String query, Map<String, Object> parameterValues) throws MException;

    /**
     * Return the amount of objects in the database.
     *
     * @param query
     * @return count the objects by query
     * @throws MException
     */
    long count(AQuery<T> query) throws MException;

    /**
     * Create a new instance of the type.
     *
     * @return new object
     * @throws Exception
     */
    T newInstance() throws Exception;

    /**
     * Delete the persistent representation of the object.
     *
     * @param object
     * @throws MException
     */
    void deleteObject(Object object) throws MException;

    /**
     * Returns the type of the attribute.
     *
     * @param name
     * @return Type of attribute
     */
    Class<?> getAttributeType(String name);

    /**
     * Returns true if the attribute is a primary key.
     *
     * @param name
     * @return true if primary java type
     */
    boolean isPrimaryKey(String name);

    /**
     * Returns true if the attribute will be stored in database.
     *
     * @param name
     * @return true if is persistent
     */
    boolean isPersistent(String name);

    /**
     * Returns a technical or mapped name of the attribute (most time the same as the name).
     *
     * @param name
     * @return the technical name
     */
    String getTechnicalName(String name);

    /**
     * Force the save of the object. If the method is not supported it should save the object in a
     * default way.
     *
     * @param object
     * @param raw Do not fire events.
     * @throws MException
     */
    void saveObjectForce(Object object, boolean raw) throws MException;

    /**
     * Store the object in the database. If the object is not already persistent it will be created.
     *
     * @param object
     * @throws MException
     */
    void saveObject(Object object) throws MException;

    /**
     * Return the requested object by primary key.
     *
     * @param keys
     * @param key Primary key
     * @return The corresponding object or null
     * @throws MException
     */
    T getObject(String... keys) throws MException;

    /**
     * Load all entries.
     *
     * @return A collection of all entries
     * @throws MException
     */
    DbCollection<T> getAll() throws MException;
}
