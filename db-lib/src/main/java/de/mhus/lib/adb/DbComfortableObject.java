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

import de.mhus.lib.basics.consts.GenerateHidden;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;

/**
 * A comfortable abstract object base to make the life of the developer fresh and handy. Use this,
 * if possible as base of a database object. You will love it.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DbComfortableObject extends MObject implements DbObject {

    private DbObjectHandler manager;
    private boolean persistent = false;
    private String registryName;
    private DbConnection con;

    /**
     * isAdbManaged.
     *
     * @return a boolean.
     */
    public boolean isAdbManaged() {
        return manager != null;
    }
    /**
     * Save changes.
     *
     * @throws de.mhus.lib.errors.MException if any.
     */
    public void save() throws MException {
        if (isAdbManaged() && !persistent) create(manager);
        else manager.saveObject(con, registryName, this);
    }

    /**
     * Save object if it is not managed jet then it will create the object
     *
     * @param manager a {@link de.mhus.lib.adb.DbObjectHandler} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    public void save(DbObjectHandler manager) throws MException {
        if (isAdbManaged() && persistent) save();
        else create(manager);
    }

    /**
     * Save the object if the object differs from database.
     *
     * @return true if the object was saved.
     * @throws de.mhus.lib.errors.MException if any.
     */
    public boolean saveChanged() throws MException {
        if (isAdbManaged() && !persistent) {
            create(manager);
            return true;
        } else if (manager.objectChanged(this)) {
            manager.saveObject(con, registryName, this);
            return true;
        }
        return false;
    }

    /**
     * Reload from database.
     *
     * @throws de.mhus.lib.errors.MException if any.
     */
    public void reload() throws MException {
        manager.reloadObject(con, registryName, this);
    }

    /**
     * Delete from database.
     *
     * @throws de.mhus.lib.errors.MException if any.
     */
    public void delete() throws MException {
        if (isAdbManaged()) {
            manager.deleteObject(con, registryName, this);
            persistent = false;
        }
    }

    /**
     * Create this new object in the database.
     *
     * @param manager a {@link de.mhus.lib.adb.DbObjectHandler} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    public void create(DbObjectHandler manager) throws MException {
        manager.createObject(con, this);
        persistent = true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overwrite to get the hook.
     */
    @Override
    @GenerateHidden
    public void doPreCreate(DbConnection con) {}

    /**
     * {@inheritDoc}
     *
     * <p>Overwrite to get the hook, the default behavior is to call doPostLoad().
     */
    @Override
    @GenerateHidden
    public void doPostCreate(DbConnection con) {
        doPostLoad(con);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overwrite to get the hook.
     */
    @Override
    @GenerateHidden
    public void doPreSave(DbConnection con) {}

    /**
     * {@inheritDoc}
     *
     * <p>Overwrite to get the hook.But remember, it could be called more times. It's only to store
     * the data.
     */
    @Override
    @GenerateHidden
    public void doInit(DbObjectHandler manager, String registryName, boolean isPersistent) {
        this.manager = manager;
        this.registryName = registryName;
        persistent = isPersistent;
    }

    /** {@inheritDoc} */
    @Override
    @GenerateHidden
    public boolean setDbHandler(DbObjectHandler manager) {
        if (this.manager != null) return false;
        this.manager = manager;
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isAdbPersistent() {
        return persistent;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overwrite to get the hook.
     */
    @Override
    @GenerateHidden
    public void doPreDelete(DbConnection con) {}

    /**
     * {@inheritDoc}
     *
     * <p>Overwrite to get the hook.
     */
    @Override
    @GenerateHidden
    public void doPostLoad(DbConnection con) {}

    /**
     * {@inheritDoc}
     *
     * <p>Overwrite to get the hook.
     */
    @Override
    @GenerateHidden
    public void doPostDelete(DbConnection con) {}

    /** {@inheritDoc} */
    @Override
    public DbObjectHandler getDbHandler() {
        return manager;
    }

    /**
     * isAdbChanged.
     *
     * @return a boolean.
     * @throws de.mhus.lib.errors.MException if any.
     */
    public boolean isAdbChanged() throws MException {
        return isAdbManaged() && (!persistent || manager.objectChanged(this));
    }
}
