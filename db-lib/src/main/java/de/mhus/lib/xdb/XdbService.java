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

import de.mhus.lib.adb.DbCollection;
import de.mhus.lib.adb.Persistable;
import de.mhus.lib.adb.query.AQuery;
import de.mhus.lib.core.pojo.PojoModelFactory;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.NotFoundException;

public interface XdbService {

    boolean isConnected();

    List<String> getTypeNames();

    <T> XdbType<T> getType(String name) throws NotFoundException;

    String getSchemaName();

    //	String getDataSourceName();

    void updateSchema(boolean cleanup) throws MException;

    void connect() throws Exception;

    default <T> T getObjectByQualification(AQuery<T> query) throws MException {
        @SuppressWarnings("unchecked")
        XdbType<T> type = (XdbType<T>) getType(query.getType());
        return type.getObjectByQualification(query);
    }

    default <T> DbCollection<T> getByQualification(AQuery<T> query) throws MException {
        @SuppressWarnings("unchecked")
        XdbType<T> type = (XdbType<T>) getType(query.getType());
        return type.getByQualification(query);
    }

    default <T extends Persistable> DbCollection<T> getAll(Class<T> type) throws MException {
        XdbType<T> xType = (XdbType<T>) getType(type);
        return xType.getAll();
    }

    default <T> long count(AQuery<T> query) throws MException {
        @SuppressWarnings("unchecked")
        XdbType<T> type = (XdbType<T>) getType(query.getType());
        return type.count(query);
    }

    <T> XdbType<T> getType(Class<T> type) throws NotFoundException;

    <T extends Persistable> T inject(T object);

    <T> T getObject(Class<T> clazz, Object... keys) throws MException;

    PojoModelFactory getPojoModelFactory();

    String getDataSourceName();

    void delete(Persistable object) throws MException;

    void save(Persistable object) throws MException;
}
