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

import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;

/**
 * DbObjectHandler interface.
 *
 * @author mikehummel
 * @version $Id: $Id
 * @since 3.3.0
 */
public interface DbObjectHandler {

    /**
     * saveObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param registryName a {@link java.lang.String} object.
     * @param obj a {@link java.lang.Object} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    void saveObject(DbConnection con, String registryName, Object obj) throws MException;

    /**
     * objectChanged.
     *
     * @param obj a {@link java.lang.Object} object.
     * @return a boolean.
     * @throws de.mhus.lib.errors.MException if any.
     */
    boolean objectChanged(Object obj) throws MException;

    /**
     * reloadObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param registryName a {@link java.lang.String} object.
     * @param obj a {@link java.lang.Object} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    void reloadObject(DbConnection con, String registryName, Object obj) throws MException;

    /**
     * deleteObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param registryName a {@link java.lang.String} object.
     * @param obj a {@link java.lang.Object} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    void deleteObject(DbConnection con, String registryName, Object obj) throws MException;

    /**
     * createObject.
     *
     * @param con a {@link de.mhus.lib.sql.DbConnection} object.
     * @param obj a {@link java.lang.Object} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    void createObject(DbConnection con, Object obj) throws MException;
}
