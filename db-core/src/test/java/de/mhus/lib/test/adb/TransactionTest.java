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
package de.mhus.lib.test.adb;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbManagerJdbc;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.adb.DbTransaction;
import de.mhus.lib.adb.transaction.NestedTransactionException;
import de.mhus.lib.core.MThread;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.core.config.MConfig;
import de.mhus.lib.core.util.Value;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DbPoolBundle;
import de.mhus.lib.test.adb.model.TransactionDummy;
import de.mhus.lib.test.adb.model.TransactionSchema;

public class TransactionTest {

    public DbPoolBundle createPool(String name) {
        IConfig cconfig = new MConfig();
        IConfig cdb = cconfig.createObject("test");

        cdb.setProperty("driver", "org.hsqldb.jdbcDriver");
        cdb.setProperty("url", "jdbc:hsqldb:mem:" + name);
        cdb.setProperty("user", "sa");
        cdb.setProperty("pass", "");

        DbPoolBundle pool = new DbPoolBundle(cconfig, null);
        return pool;
    }

    public DbManager createManager() throws Exception {
        DbPool pool = createPool("transactionModel").getPool("test");
        DbSchema schema = new TransactionSchema();
        DbManager manager = new DbManagerJdbc("", pool, schema);
        return manager;
    }

    @Test
    public void testLock() throws Exception {

        DbManager manager = createManager();
        final TransactionDummy obj1 = manager.inject(new TransactionDummy());
        final TransactionDummy obj2 = manager.inject(new TransactionDummy());
        final TransactionDummy obj3 = manager.inject(new TransactionDummy());

        obj1.save();
        obj2.save();
        obj3.save();

        DbTransaction.lockDefault(obj1, obj2); // simple lock
        DbTransaction.releaseLock();

        DbTransaction.releaseLock(); // one more should be ok - robust code

        DbTransaction.lockDefault(obj1, obj2);
        try {
            DbTransaction.lockDefault(
                    obj3); // nested not locked should fail, can't lock two times - philosophers
            // deadlock
            DbTransaction.releaseLock();
            fail("Nested Transaction Not Allowed");
        } catch (NestedTransactionException e) {
            System.out.println(e);
        }
        DbTransaction.releaseLock();

        DbTransaction.lockDefault(obj1, obj2);
        DbTransaction.lockDefault(
                obj1); // nested is ok as long as it is already locked - no philosophers problem
        DbTransaction.releaseLock();
        DbTransaction.releaseLock();

        // concurrent locking ...
        DbTransaction.lockDefault(obj1, obj2);

        final Value<Boolean> done = new Value<>(false);
        final Value<String> fail = new Value<>();

        new MThread(
                        new Runnable() {

                            @Override
                            public void run() {
                                // concurrent
                                try {
                                    DbTransaction.lock(2000, obj1, obj2);
                                    fail.setValue("Concurrent Lock Possible");
                                    return;
                                } catch (Throwable t) {
                                    System.out.println(t);
                                } finally {
                                    DbTransaction.releaseLock();
                                }

                                // not concurrent
                                DbTransaction.lock(2000, obj3);
                                DbTransaction.releaseLock();

                                done.setValue(true);
                            }
                        })
                .start();

        while (done.getValue() == false && fail.getValue() == null) MThread.sleep(200);

        if (fail.getValue() != null) fail(fail.getValue());

        DbTransaction.releaseLock();
    }
}
