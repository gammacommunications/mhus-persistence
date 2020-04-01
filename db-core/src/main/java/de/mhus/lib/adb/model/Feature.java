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

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;
import de.mhus.lib.sql.DbResult;

public abstract class Feature extends MObject {

    protected DbManager manager;
    protected Table table;

    public void init(DbManager manager, Table table) throws MException {
        this.manager = manager;
        this.table = table;
        doInit();
    }

    protected void doInit() throws MException {}

    public void preCreateObject(DbConnection con, Object object) throws Exception {}

    public void preSaveObject(DbConnection con, Object object) throws Exception {}

    public void preGetObject(DbConnection con, DbResult ret) throws Exception {}

    public void postGetObject(DbConnection con, Object obj) throws Exception {}

    public void preFillObject(Object obj, DbConnection con, DbResult res) throws Exception {}

    public void deleteObject(DbConnection con, Object object) throws Exception {}

    public Object getValue(Object obj, Field field, Object val) throws Exception {
        return val;
    }

    public Object setValue(Object obj, Field field, Object value) throws Exception {
        return value;
    }

    public void postFillObject(Object obj, DbConnection con) throws Exception {}

    public void postCreateObject(DbConnection con, Object object) throws Exception {}

    public void postSaveObject(DbConnection con, Object object) throws Exception {}
}
