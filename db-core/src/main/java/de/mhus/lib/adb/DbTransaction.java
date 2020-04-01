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

import de.mhus.lib.adb.transaction.TransactionLock;
import de.mhus.lib.adb.transaction.TransactionPool;
import de.mhus.lib.annotations.adb.DbTransactionable;
import de.mhus.lib.annotations.adb.TransactionConnection;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.logging.MLogUtil;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.TimeoutRuntimeException;

/**
 * Allow transaction and lock manageent with adb framework. This implementation should be used if
 * you not need to synchronize transactions with other resources. A JTA implementation is not planed
 * but can be implemented on top of this implementation. The transaction is based on the current
 * thread. If you leave the thread you will also leave the current transaction.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DbTransaction {

    /** Constant <code>DEFAULT_TIMEOUT=MTimeInterval.MINUTE_IN_MILLISECOUNDS * 10</code> */
    public static final long DEFAULT_TIMEOUT = MPeriod.MINUTE_IN_MILLISECOUNDS * 10;

    /**
     * lock accept only nested locks with already locked objects.
     *
     * @param objects a {@link de.mhus.lib.adb.Persistable} object.
     * @return A lock object
     * @throws de.mhus.lib.errors.TimeoutRuntimeException if any.
     */
    public static DbLock lock(Persistable... objects) throws TimeoutRuntimeException {
        return lock(DEFAULT_TIMEOUT, objects);
    }

    /**
     * lock and accept all nested locks.
     *
     * @param objects a {@link de.mhus.lib.adb.Persistable} object.
     * @return The Lock
     * @throws de.mhus.lib.errors.TimeoutRuntimeException if any.
     */
    public static DbLock relaxedLock(Persistable... objects) throws TimeoutRuntimeException {
        return relaxedLock(DEFAULT_TIMEOUT, objects);
    }

    /**
     * lock accept only nested locks with already locked objects.
     *
     * @param timeout a long.
     * @param objects a {@link de.mhus.lib.adb.Persistable} object.
     * @return The Lock
     * @throws de.mhus.lib.errors.TimeoutRuntimeException if any.
     */
    public static DbLock lock(long timeout, Persistable... objects) throws TimeoutRuntimeException {
        TransactionPool.instance().lock(timeout, new TransactionLock(false, objects));
        return new DbLock(objects);
    }

    /**
     * lock and accept all nested locks.
     *
     * @param timeout a long.
     * @param objects a {@link de.mhus.lib.adb.Persistable} object.
     * @return The Lock
     * @throws de.mhus.lib.errors.TimeoutRuntimeException if any.
     */
    public static DbLock relaxedLock(long timeout, Persistable... objects)
            throws TimeoutRuntimeException {
        TransactionPool.instance().lock(timeout, new TransactionLock(true, objects));
        return new DbLock(objects);
    }

    /**
     * lock.
     *
     * @param manager a {@link de.mhus.lib.adb.DbManager} object.
     * @param objects a {@link de.mhus.lib.adb.Persistable} object.
     * @return The Lock
     * @throws de.mhus.lib.errors.TimeoutRuntimeException if any.
     */
    public static DbLock lock(DbManager manager, Persistable... objects)
            throws TimeoutRuntimeException {
        return lock(manager, DEFAULT_TIMEOUT, objects);
    }

    /**
     * lock.
     *
     * @param manager a {@link de.mhus.lib.adb.DbManager} object.
     * @param timeout a long.
     * @param objects a {@link de.mhus.lib.adb.Persistable} object.
     * @return The Lock
     * @throws de.mhus.lib.errors.TimeoutRuntimeException if any.
     */
    public static DbLock lock(DbManager manager, long timeout, Persistable... objects)
            throws TimeoutRuntimeException {
        TransactionPool.instance().lock(timeout, new TransactionLock(manager, false, objects));
        return new DbLock(objects);
    }

    /** Release all locked object ids This method will never throw an Throwable. */
    public static void releaseLock() {
        try {
            TransactionPool.instance().releaseLock();
        } catch (Throwable t) {
            try {
                MLogUtil.log().e(t);
            } catch (Throwable t2) {
                t.printStackTrace();
                t2.printStackTrace();
            }
        }
    }

    /**
     * Close and remove all existing transaction connections. This method will never throw an
     * Throwable.
     */
    public static void releaseEncapsulate() {
        try {
            TransactionPool.instance().releaseEncapsulate();
        } catch (Throwable t) {
            try {
                MLogUtil.log().e(t);
            } catch (Throwable t2) {
                t.printStackTrace();
                t2.printStackTrace();
            }
        }
    }

    /**
     * Get an Connection for owner and hold the connection for all activities. If the owner is
     * already in transaction it will not be replaced and return true. The the owner can't provide a
     * connection this method will return false because no transaction connection was set.
     *
     * @param owner The connection provider
     * @return true if everything is ok
     */
    public static boolean encapsulate(DbTransactionable owner) {
        return TransactionPool.instance().encapsulate(owner);
    }

    /**
     * Return true if a transaction connection from this owner is available.
     *
     * @param owner
     * @return true if in transaction
     */
    public static boolean isInTransaction(DbTransactionable owner) {
        return TransactionPool.instance().isInTransaction(owner);
    }

    /**
     * Return the deposit connection of this owner or null.
     *
     * @param owner
     * @return the connection
     */
    public static TransactionConnection getConnection(DbTransactionable owner) {
        return TransactionPool.instance().getConnection(owner);
    }

    /**
     * Commit and close all transaction connections This method will never throw an Throwable.
     *
     * @return true If no error was thrown. If false some connections can be committed other not.
     *     But it will be released in every case.
     */
    public static boolean commitAndRelease() {
        try {
            return TransactionPool.instance().commitAndRelease();
        } catch (Throwable t) {
            try {
                MLogUtil.log().e(t);
            } catch (Throwable t2) {
                t.printStackTrace();
                t2.printStackTrace();
            }
            return false;
        }
    }

    /**
     * Roll back and close all transaction connections. This method will never throw an Throwable.
     *
     * @return If no error was thrown. If false some connections can be rolled back other not. But
     *     it will be released in every case.
     * @throws MException
     */
    public static boolean rollbackAndRelease() throws MException {
        try {
            return TransactionPool.instance().rollbackAndRelease();
        } catch (Throwable t) {
            try {
                MLogUtil.log().e(t);
            } catch (Throwable t2) {
                t.printStackTrace();
                t2.printStackTrace();
            }
            return false;
        }
    }

    /**
     * Commit all transaction connections, do not remove the transaction. All following activities
     * will be done in the next transaction. This method will never throw an Throwable.
     *
     * @return If no error was thrown. If false some connections can be committed other not.
     */
    public static boolean commitWithoutRelease() {
        try {
            return TransactionPool.instance().commit();
        } catch (Throwable t) {
            try {
                MLogUtil.log().e(t);
            } catch (Throwable t2) {
                t.printStackTrace();
                t2.printStackTrace();
            }
            return false;
        }
    }

    /**
     * Roll back all transaction connections, do not remove the transaction. All following
     * activities will be done in the next transaction. This method will never throw an Throwable.
     *
     * @return If no error was thrown. If false some connections can be rolled back other not.
     */
    public static boolean rollbackWithoutRelease() {
        try {
            return TransactionPool.instance().rollback();
        } catch (Throwable t) {
            try {
                MLogUtil.log().e(t);
            } catch (Throwable t2) {
                t.printStackTrace();
                t2.printStackTrace();
            }
            return false;
        }
    }
}
