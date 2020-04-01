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
package de.mhus.lib.test.adb.model;

import java.util.HashMap;
import java.util.List;

import de.mhus.lib.adb.DbAccessManager;
import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbManagerJdbc;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.adb.Persistable;
import de.mhus.lib.adb.model.Table;
import de.mhus.lib.errors.AccessDeniedException;
import de.mhus.lib.errors.MException;
import de.mhus.lib.sql.DbConnection;

public class BookStoreSchema extends DbSchema {

    public boolean switchStore = false;

    @Override
    public void doFillNameMapping(HashMap<String, Object> nameMapping) {
        super.doFillNameMapping(nameMapping);
        System.out.println(nameMapping);
    }

    @Override
    public void doMigrate(DbManager dbManager, long currentVersion) throws MException {
        if (currentVersion == 0) {
            Person p = new Person();
            p.setName("Hausmeister Krause");
            dbManager.createObject(p);
            dbManager.getSchemaProperties().set(DbManagerJdbc.DATABASE_VERSION, "1");
        }
    }

    @Override
    public DbAccessManager getAccessManager(Table c) {
        if (c.getClazz() == Finances.class) {
            return new DbAccessManager() {

                @Override
                public void hasAccess(
                        DbManager manager,
                        Table c,
                        DbConnection con,
                        Object object,
                        DbAccessManager.ACCESS right)
                        throws AccessDeniedException {

                    Finances f = (Finances) object;

                    String conf = f.getConfidential();
                    if (conf != null) {
                        if (right == DbAccessManager.ACCESS.UPDATE && conf.indexOf("write") >= 0)
                            throw new AccessDeniedException("access denied");

                        if (right == DbAccessManager.ACCESS.DELETE && conf.indexOf("remove") >= 0)
                            throw new AccessDeniedException("access denied");

                        if (right == DbAccessManager.ACCESS.READ && conf.indexOf("read") >= 0)
                            throw new AccessDeniedException("access denied");
                    }
                    // set new acl if needed
                    if (f.getNewConfidential() != null) f.setConfidential(f.getNewConfidential());
                }
            };
        }
        return null;
    }

    @Override
    public void findObjectTypes(List<Class<? extends Persistable>> list) {
        list.add(Book.class);
        list.add(Person.class);
        list.add(Person2.class);
        list.add(Finances.class);
        list.add(Regal.class);

        if (switchStore) list.add(de.mhus.lib.test.adb.model2.Store.class);
        else list.add(Store.class);
    }

    @Override
    public void authorizeReadAttributes(
            DbConnection con,
            DbManager dbManagerJdbc,
            Class<?> clazz,
            String registryName,
            String attribute) {}
}
