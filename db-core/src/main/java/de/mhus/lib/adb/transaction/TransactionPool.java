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
package de.mhus.lib.adb.transaction;

import de.mhus.lib.annotations.adb.DbTransactionable;
import de.mhus.lib.annotations.adb.TransactionConnection;
import de.mhus.lib.core.MLog;

public class TransactionPool extends MLog {

    private static TransactionPool instance;
    private ThreadLocal<LockBase> lock = new ThreadLocal<>();
    private ThreadLocal<Encapsulation> encapsulate = new ThreadLocal<>();

    public static synchronized TransactionPool instance() {
        if (instance == null) instance = new TransactionPool();
        return instance;
    }

    /**
     * Return current transaction, if cascaded transactions, return the last/current
     *
     * @return x
     */
    public LockBase getLock() {
        //		synchronized (pool) {
        LockBase out = lock.get();
        LockBase nested = out.getNested();
        if (nested != null) return nested;
        return out;
        //		}
    }

    /**
     * Return current transaction, if cascaded transactions, return the base
     *
     * @return x
     */
    public LockBase getLockBase() {
        //		synchronized (pool) {
        LockBase out = lock.get();
        return out;
        //		}
    }

    public void releaseLock() {
        //		synchronized (pool) {
        LockBase out = lock.get();
        if (out == null) return;
        LockBase nested = out.popNestedLock();
        if (nested == null) {
            lock.remove();
            out.release();
        }
        //		}
    }

    public void lock(long timeout, LockBase transaction) {
        //		synchronized (pool) {
        LockBase current = lock.get();
        if (current != null) current.pushNestedLock(transaction);
        else {
            lock.set(transaction);
            transaction.lock(timeout);
        }
        //		}
    }

    public boolean encapsulate(DbTransactionable owner) {
        //		synchronized (encapsulate) {
        Encapsulation enc = encapsulate.get();
        if (enc == null) {
            enc = new Encapsulation();
            encapsulate.set(enc);
        }
        return enc.append(owner);
        //		}
    }

    public void releaseEncapsulate() {
        Encapsulation enc = encapsulate.get();
        if (enc == null) return;
        enc.clear();
        encapsulate.remove();
    }

    public TransactionConnection getConnection(DbTransactionable owner) {
        Encapsulation enc = encapsulate.get();
        if (enc == null) return null;
        return enc.getCurrent(owner);
    }

    public boolean commitAndRelease() {
        boolean res = commit();
        releaseEncapsulate();
        return res;
    }

    public boolean rollbackAndRelease() {
        boolean res = rollback();
        releaseEncapsulate();
        return res;
    }

    public boolean commit() {
        Encapsulation enc = encapsulate.get();
        if (enc == null) {
            log().d("encapsulate not enabled - ignore commit");
            return false;
        }
        return enc.commit();
    }

    public boolean rollback() {
        Encapsulation enc = encapsulate.get();
        if (enc == null) return false;
        return enc.rollback();
    }

    public void clear() {
        lock.remove();
        Encapsulation enc = encapsulate.get();
        if (enc != null) enc.clear();
        encapsulate.remove();
    }

    public boolean isInTransaction(DbTransactionable owner) {
        Encapsulation enc = encapsulate.get();
        if (enc == null) return false;
        return enc.isInTransaction(owner);
    }
}
