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
package de.mhus.lib.adb.transaction;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbObject;
import de.mhus.lib.adb.DbTransaction;
import de.mhus.lib.adb.model.Field;
import de.mhus.lib.adb.model.Table;
import de.mhus.lib.core.MCast;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.cfg.CfgBoolean;
import de.mhus.lib.core.logging.ITracer;
import de.mhus.lib.errors.NotSupportedException;
import de.mhus.lib.errors.TimeoutRuntimeException;

/**
 * TransactionLock class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class TransactionLock extends LockBase {

    private static final CfgBoolean CFG_TRACE_CALLER =
            new CfgBoolean(DbTransaction.class, "traceTransactionCallers", false);
    private Object[] objects;
    private DbManager manager;
    private boolean locked;
    private TreeMap<String, Object> orderedKeys;
    private String stacktrace;
    private boolean relaxed;

    /**
     * Constructor for TransactionLock.
     *
     * @param manager a {@link de.mhus.lib.adb.DbManager} object.
     * @param relaxed
     * @param objects
     */
    public TransactionLock(DbManager manager, boolean relaxed, Object... objects) {
        this.manager = manager;
        this.objects = objects;
        this.relaxed = relaxed;
        if (CFG_TRACE_CALLER.value())
            this.stacktrace =
                    "\n"
                            + MCast.toString(
                                    "TransactionLock " + Thread.currentThread().getId() + " " + ITracer.get().getCurrentId(), Thread.currentThread().getStackTrace());
        else
            this.stacktrace = "TransactionLock " + Thread.currentThread().getId() + " " + ITracer.get().getCurrentId();
    }

    /**
     * Constructor for TransactionLock.
     *
     * @param relaxed
     * @param objects
     */
    public TransactionLock(boolean relaxed, Object... objects) {
        this(null, relaxed, objects);
        for (Object o : objects)
            if (o instanceof DbObject) {
                manager = (DbManager) ((DbObject) o).getDbHandler();
                break;
            }
    }

    /** {@inheritDoc} */
    @Override
    public void lock(long timeout) throws TimeoutRuntimeException {
        if (objects == null)
            throw new NotSupportedException("Transaction already gone " + stacktrace);
        if (manager == null)
            throw new NotSupportedException(
                    "DbManager not found, need direct manager or DbObject implementation to grep the manager "
                            + stacktrace);
        LockStrategy strategy = manager.getSchema().getLockStrategy();
        if (strategy == null) return;

        getLockKeys();

        long start = System.currentTimeMillis();
        ArrayList<Map.Entry<String, Object>> done = new ArrayList<>(orderedKeys.size());
        for (Map.Entry<String, Object> entry : orderedKeys.entrySet()) {
            try {
                strategy.lock(entry.getValue(), entry.getKey(), this, timeout);
                done.add(entry);
            } catch (Throwable t) {
                log().d(t);
            }
            if (System.currentTimeMillis() - start > timeout) {
                for (Map.Entry<String, Object> entry2 : done) {
                    try {
                        strategy.releaseLock(entry2.getValue(), entry2.getKey(), this);
                    } catch (Throwable t) {
                        log().d(t);
                    }
                }
                throw new TimeoutRuntimeException(orderedKeys);
            }
        }

        locked = true;
    }

    /**
     * createKey.
     *
     * @param o a {@link de.mhus.lib.adb.Object} object.
     * @return a {@link java.lang.String} object.
     */
    protected String createKey(Object o) {
        // find db manager of the object, fallback is my manager
        DbManager m = manager;
        if (o instanceof DbObject) m = (DbManager) ((DbObject) o).getDbHandler();

        String regName = m.getRegistryName(o);
        Table table = m.getTable(regName);
        StringBuilder key = new StringBuilder().append(regName);
        for (Field pKey : table.getPrimaryKeys()) {
            String value = "";
            try {
                value = String.valueOf(pKey.get(o));
            } catch (Exception e) {
                log().d(e);
            }
            key.append(",").append(value);
        }
        return key.toString();
    }

    /** {@inheritDoc} */
    @Override
    public void release() {
        if (!locked || objects == null || manager == null) return;

        LockStrategy strategy = manager.getSchema().getLockStrategy();
        if (strategy == null) return;

        for (Object o : objects) {
            String key = createKey(o);
            try {
                strategy.releaseLock(o, key, this);
            } catch (Throwable t) {
                log().d(t);
            }
        }

        manager = null;
        objects = null;
        locked = false;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void pushNestedLock(LockBase transaction) {
        // validate lock objects
        // this: current locked
        // transaction: request lock

        if (isRelaxed()) return; // I'm relaxed means I accept all nested locks

        Set<String> keys = transaction.getLockKeys();
        getLockKeys();
        if (keys != null) {
            for (String key : keys) {
                if (!orderedKeys.containsKey(key)) {
                    throw new NestedTransactionException(
                            "Nested key not locked in MainLock, dead lock possible: "
                                    + key
                                    + ", actuall locked: "
                                    + orderedKeys
                                    + stacktrace);
                }
            }
        }
        super.pushNestedLock(transaction);
    }

    /** {@inheritDoc} */
    @Override
    protected void finalize() {
        // TODO error message !
        release();
    }

    /** {@inheritDoc} */
    @Override
    public DbManager getDbManager() {
        return manager;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Set<String> getLockKeys() {
        if (orderedKeys == null) {
            orderedKeys = new TreeMap<>();
            if (objects != null) {
                for (Object o : objects) {
                    if (o == null) continue;
                    String key = createKey(o);
                    orderedKeys.put(key, o);
                }
            }
        }
        return orderedKeys.keySet();
    }

    public boolean isRelaxed() {
        return relaxed;
    }

    @Override
    protected boolean isLocked() {
        if (objects == null || orderedKeys == null) return false;

        LockStrategy strategy = manager.getSchema().getLockStrategy();
        if (strategy == null) return false;

        boolean locked = true;
        for (Map.Entry<String, Object> entry : orderedKeys.entrySet()) {
            if (!strategy.isLockedByOwner(entry.getValue(), entry.getKey(), this)) {
                locked = false;
                break;
            }
        }
        if (!locked) {
            for (Map.Entry<String, Object> entry : orderedKeys.entrySet()) {
                if (strategy.isLockedByOwner(entry.getValue(), entry.getKey(), this)) {
                    try {
                        strategy.releaseLock(entry.getValue(), entry.getKey(), this);
                    } catch (Throwable t) {
                        log().d(entry.getKey(), t);
                    }
                }
            }
            manager = null;
            objects = null;
            locked = false;
        }

        return locked;
    }
    
    @Override
    public String toString() {
        return MSystem.toString(this, locked, relaxed, orderedKeys, stacktrace);
    }

    @Override
    public String getName() {
        return MSystem.getObjectId(this);
    }
}
