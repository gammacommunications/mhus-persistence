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
package de.mhus.lib.adb;

import de.mhus.lib.sql.DbConnection;

/**
 * Interface to allow send hooks to the object.
 *
 * @author mikehummel
 */
public interface DbObject extends Persistable {

    void doPreCreate(DbConnection con);

    void doPreSave(DbConnection con);

    void doInit(DbObjectHandler manager, String registryName, boolean isPersistent);

    void doPreDelete(DbConnection con);

    void doPostLoad(DbConnection con);

    void doPostCreate(DbConnection con);

    void doPostDelete(DbConnection con);

    boolean isAdbPersistent();

    DbObjectHandler getDbHandler();

    /**
     * Handler can be set only one time. Return true if the hander is set. Or false if the handler
     * was already present.
     *
     * @param manager
     * @return x
     */
    boolean setDbHandler(DbObjectHandler manager);
}
