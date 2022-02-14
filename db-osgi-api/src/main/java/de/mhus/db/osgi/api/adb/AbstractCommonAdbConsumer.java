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

import java.util.UUID;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.basics.RC;
import de.mhus.lib.core.MLog;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.MValidator;
import de.mhus.lib.core.cfg.CfgLong;
import de.mhus.lib.errors.MException;
import de.mhus.lib.xdb.XdbService;

public abstract class AbstractCommonAdbConsumer extends MLog implements CommonDbConsumer {

    public static CfgLong CFG_TIMEOUT =
            new CfgLong(
                    CommonDbConsumer.class, "cacheTimeout", MPeriod.MINUTE_IN_MILLISECONDS * 5);
    protected DbManager manager;

    @Override
    public final void doInitialize(XdbService dbService) {
        manager = (DbManager) dbService;
        doInitialize();
    }

    protected abstract void doInitialize();

    @Override
    public boolean canCreate(Object obj) throws MException {
        return true;
    }

    @Override
    public boolean canRead(Object obj) throws MException {
        return true;
    }

    @Override
    public boolean canUpdate(Object obj) throws MException {
        return true;
    }

    @Override
    public boolean canDelete(Object obj) throws MException {
        return true;
    }

    @Override
    public Object getObject(String type, UUID id) throws MException {
        try {
            Class<?> clazz = Class.forName(type, true, this.getClass().getClassLoader());
            if (clazz != null) {
                return manager.getObject(clazz, id);
            }
        } catch (Throwable t) {
            throw new MException(RC.STATUS.ERROR, "type error", type, t);
        }
        throw new MException(RC.STATUS.ERROR, "unknown type {1}", type);
    }

    @Override
    public Object getObject(String type, String id) throws MException {
        try {
            if (MValidator.isUUID(id)) return getObject(type, UUID.fromString(id));
        } catch (Throwable t) {
            throw new MException(RC.STATUS.ERROR, "type error", type, t);
        }
        throw new MException(RC.STATUS.ERROR, "unknown type {1}", type);
    }

    public DbManager getManager() {
        return manager;
    }
}
