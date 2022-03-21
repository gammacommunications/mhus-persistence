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
package de.mhus.lib.test.adb;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbManagerJdbc;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.adb.DbTransaction;
import de.mhus.lib.adb.transaction.MemoryLockStrategy;
import de.mhus.lib.adb.transaction.NestedTransactionException;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.MThread;
import de.mhus.lib.core.node.INode;
import de.mhus.lib.core.node.MNode;
import de.mhus.lib.core.util.Value;
import de.mhus.lib.sql.DbPool;
import de.mhus.lib.sql.DbPoolBundle;
import de.mhus.lib.test.adb.model.TransactionDummy;
import de.mhus.lib.test.adb.model.TransactionSchema;
import de.mhus.lib.tests.TestUtil;

public class TransactionTest {

    private static DbManager manager;
    private static TransactionDummy obj1;
    private static TransactionDummy obj2;
    private static TransactionDummy obj3;

    public static DbPoolBundle createPool(String name) {
        INode cconfig = new MNode();
        INode cdb = cconfig.createObject("test");

        cdb.setProperty("driver", "org.hsqldb.jdbcDriver");
        cdb.setProperty("url", "jdbc:hsqldb:mem:" + name);
        cdb.setProperty("user", "sa");
        cdb.setProperty("password", "");

        DbPoolBundle pool = new DbPoolBundle(cconfig, null);
        return pool;
    }

    public static DbManager createManager() throws Exception {
        DbPool pool = createPool("transactionModel").getPool("test");
        DbSchema schema = new TransactionSchema();
        DbManager manager = new DbManagerJdbc("", pool, null, schema);
        return manager;
    }

    @BeforeAll
    public static void begin() throws Exception {
        TestUtil.clearCfg();
        TestUtil.setCfg(DbTransaction.class, "traceTransactionCallers", "true");

        manager = createManager();
        obj1 = manager.inject(new TransactionDummy());
        obj2 = manager.inject(new TransactionDummy());
        obj3 = manager.inject(new TransactionDummy());

        obj1.save();
        obj2.save();
        obj3.save();
    }

    @Test
    public void testSimpleLock() throws Exception {

        DbTransaction.lockDefault(obj1, obj2); // simple lock
        DbTransaction.releaseLock();

        DbTransaction.releaseLock(); // one more should be ok - robust code
    }

    @Test
    public void testNestedLockExclude() throws Exception {

        // Test nested locks - exclude

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
    }

    @Test
    public void testSimpleLockInclude() throws Exception {

        // test nested locks - include

        DbTransaction.lockDefault(obj1, obj2);
        DbTransaction.lockDefault(
                obj1); // nested is ok as long as it is already locked - no philosophers problem
        DbTransaction.releaseLock();
        DbTransaction.releaseLock();
    }

    @Test
    public void testConcurrentLock() throws Exception {

        // test concurrent locking
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

                                done.setValue(true);
                            }
                        })
                .start();

        while (done.getValue() == false && fail.getValue() == null) MThread.sleep(200);

        if (fail.getValue() != null) fail(fail.getValue());

        DbTransaction.releaseLock();
    }

    @Test
    public void testConcurrentLockTimeout() throws Exception {

        // test concurrent locking with lock timeout
        try {
            ((MemoryLockStrategy) manager.getSchema().getLockStrategy()).setMaxLockAge(1000);
            DbTransaction.lockDefault(obj1, obj2);

            final Value<Boolean> done = new Value<>(false);
            final Value<String> fail = new Value<>();

            MThread.sleep(2000);

            new MThread(
                            new Runnable() {

                                @Override
                                public void run() {
                                    // concurrent
                                    try {
                                        DbTransaction.lock(2000, obj1, obj2);
                                    } catch (Throwable t) {
                                        fail.setValue("Lock was not cleaned");
                                        System.out.println(t);
                                    } finally {
                                        DbTransaction.releaseLock();
                                    }
                                    done.setValue(true);
                                }
                            })
                    .start();

            while (done.getValue() == false && fail.getValue() == null) MThread.sleep(200);

            if (fail.getValue() != null) fail(fail.getValue());

            DbTransaction.releaseLock();

        } finally {
            ((MemoryLockStrategy) manager.getSchema().getLockStrategy())
                    .setMaxLockAge(MPeriod.HOUR_IN_MILLISECONDS); // set back to 'long'
        }
    }

    @Test
    public void testLockTimeout() throws Exception {

        // test lock with timeout of old transaction - old transaction will vanish
        try {
            ((MemoryLockStrategy) manager.getSchema().getLockStrategy()).setMaxLockAge(1000);
            DbTransaction.lockDefault(obj1, obj2);

            MThread.sleep(2000);
            DbTransaction.lockDefault(obj3);

            DbTransaction.releaseLock();

        } finally {
            ((MemoryLockStrategy) manager.getSchema().getLockStrategy())
                    .setMaxLockAge(MPeriod.HOUR_IN_MILLISECONDS); // set back to 'long'
        }
    }
}
