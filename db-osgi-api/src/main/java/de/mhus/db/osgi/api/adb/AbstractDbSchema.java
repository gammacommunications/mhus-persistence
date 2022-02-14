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

import java.util.HashMap;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.adb.model.Table;
import de.mhus.lib.adb.transaction.MemoryLockStrategy;
import de.mhus.lib.core.MApi;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.aaa.Aaa;
import de.mhus.lib.errors.AccessDeniedException;
import de.mhus.lib.sql.DbConnection;

public abstract class AbstractDbSchema extends DbSchema {

    //    private Log trace =
    //            new SopFileLogger(
    //                    MApi.getCfg(DbManagerService.class).getExtracted("traceLoggerName", "db"),
    //                    getClass().getCanonicalName());

    public AbstractDbSchema() {
        //        trace.i("start");
        lockStrategy = new MemoryLockStrategy();
        ((MemoryLockStrategy) lockStrategy)
                .setMaxLockAge(
                        MApi.getCfg(AdbService.class)
                                .getLong("maxLockAge", MPeriod.MINUTE_IN_MILLISECONDS * 5));
    }

    @Override
    public void authorizeSaveForceAllowed(DbConnection con, Table table, Object object, boolean raw)
            throws AccessDeniedException {
        if (!Aaa.hasAccess(Table.class, "saveforce", table.getRegistryName()))
            throw new AccessDeniedException(Aaa.getPrincipal(), table.getName(), "saveforce");
    }

    @Override
    public void authorizeUpdateAttributes(
            DbConnection con, Table table, Object object, boolean raw, String... attributeNames)
            throws AccessDeniedException {
        if (Aaa.hasAccess(Table.class, "updateattributes", table.getRegistryName())) return;
        for (String attr : attributeNames)
            if (!Aaa.hasAccess(
                    Table.class, "updateattributes", table.getRegistryName() + "_" + attr))
                throw new AccessDeniedException(
                        Aaa.getPrincipal(), table.getName(), "updateattributes", attr);
    }

    @Override
    public void authorizeReadAttributes(
            DbConnection con,
            DbManager dbManagerJdbc,
            Class<?> clazz,
            Class<?> clazz2,
            String registryName,
            String attribute) {

        if (registryName == null) {
            if (clazz2 != null) registryName = dbManagerJdbc.getRegistryName(clazz2);
            else if (clazz != null) registryName = dbManagerJdbc.getRegistryName(clazz);
        }

        if (!Aaa.hasAccess(Table.class, "readattributes", registryName + "_" + attribute)
                && !Aaa.hasAccess(Table.class, "readattributes", registryName))
            throw new AccessDeniedException(
                    Aaa.getPrincipal(), registryName, "readattributes", attribute);
    }

    @Override
    public void internalCreateObject(
            DbConnection con, String name, Object object, HashMap<String, Object> attributes) {
        super.internalCreateObject(con, name, object, attributes);
        //        trace.i("create", name, attributes, object);
    }

    @Override
    public void internalSaveObject(
            DbConnection con, String name, Object object, HashMap<String, Object> attributes) {
        super.internalSaveObject(con, name, object, attributes);
        //        trace.i("modify", name, attributes, object);
    }

    @Override
    public void internalDeleteObject(
            DbConnection con, String name, Object object, HashMap<String, Object> attributes) {
        super.internalDeleteObject(con, name, object, attributes);
        //        trace.i("delete", name, attributes, object);
    }
}
