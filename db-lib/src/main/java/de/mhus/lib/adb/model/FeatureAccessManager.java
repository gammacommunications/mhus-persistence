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
package de.mhus.lib.adb.model;

import de.mhus.lib.adb.DbAccessManager;
import de.mhus.lib.sql.DbConnection;

/**
 * FeatureAccessManager class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class FeatureAccessManager extends Feature {

    public DbAccessManager accessManager;

    /** {@inheritDoc} */
    @Override
    protected void doInit() {
        accessManager = manager.getSchema().getAccessManager(table);
    }

    /** {@inheritDoc} */
    @Override
    public void postFillObject(Object obj, DbConnection con) throws Exception {
        if (accessManager != null)
            accessManager.hasAccess(manager, table, con, obj, DbAccessManager.ACCESS.READ);
    }

    /** {@inheritDoc} */
    @Override
    public void preCreateObject(DbConnection con, Object object) throws Exception {
        if (accessManager != null)
            accessManager.hasAccess(manager, table, con, object, DbAccessManager.ACCESS.CREATE);
    }

    /** {@inheritDoc} */
    @Override
    public void preSaveObject(DbConnection con, Object object) throws Exception {
        if (accessManager != null)
            accessManager.hasAccess(manager, table, con, object, DbAccessManager.ACCESS.UPDATE);
    }

    /** {@inheritDoc} */
    @Override
    public void deleteObject(DbConnection con, Object object) throws Exception {
        if (accessManager != null)
            accessManager.hasAccess(manager, table, con, object, DbAccessManager.ACCESS.DELETE);
    }

    /** {@inheritDoc} */
    @Override
    public void postGetObject(DbConnection con, Object obj) throws Exception {
        if (accessManager != null)
            accessManager.hasAccess(manager, table, con, obj, DbAccessManager.ACCESS.READ);
    }
}
