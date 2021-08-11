/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.db.osgi.api.adb;

import java.util.List;
import java.util.UUID;

import de.mhus.lib.errors.MException;
import de.mhus.lib.xdb.XdbService;

public interface CommonDbConsumer {

    String REASON_DELETE = "delete";

    /**
     * Register supported object types for the database. Will be called multiple times and should
     * always return the same collection of classes.
     *
     * @param list
     */
    void registerObjectTypes(List<Class<? extends Object>> list);

    void doInitialize(XdbService dbService);

    void doDestroy();

    boolean canRead(Object obj) throws MException;

    boolean canUpdate(Object obj) throws MException;

    boolean canDelete(Object obj) throws MException;

    boolean canCreate(Object obj) throws MException;

    Object getObject(String type, UUID id) throws MException;

    Object getObject(String type, String id) throws MException;

    void collectReferences(Object object, ReferenceCollector collector, String reason);

    void doCleanup();

    /**
     * Is called after creation of the DbManager. Could be called multiple times if a recreation of
     * the db manager was done.
     *
     * @param manager
     * @throws Exception
     */
    void doPostInitialize(XdbService manager) throws Exception;

    public default boolean isReasonDelete(String reason) {
        return REASON_DELETE.equals(reason);
    }
}
