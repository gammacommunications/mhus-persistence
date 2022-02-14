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

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.MThread;
import de.mhus.lib.core.cfg.CfgBoolean;
import de.mhus.lib.core.cfg.CfgLong;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.TimeoutRuntimeException;

public class DbLockObjectStrategy extends LockStrategy {

    private static final CfgLong CFG_MAX_LOCK_AGE =
            new CfgLong(DbLockObjectStrategy.class, "maxLockAge", MPeriod.HOUR_IN_MILLISECONDS);
    private static final CfgLong CFG_SLEEP_TIME =
            new CfgLong(DbLockObjectStrategy.class, "sleepTime", 200);
    private static final CfgBoolean CFG_IGNORE_LOCK_OWNER =
            new CfgBoolean(DbLockObjectStrategy.class, "ignoreLockOwner", false);

    private long maxLockAge = CFG_MAX_LOCK_AGE.value();
    private long sleepTime = CFG_SLEEP_TIME.value();
    private boolean ignoreLockOwner = CFG_IGNORE_LOCK_OWNER.value();

    @Override
    public void lock(Object object, String key, LockBase transaction, long timeout) {
        DbLockObject lock = transaction.getDbManager().inject(new DbLockObject());
        //		if (key.length() > 760) key = key.substring(0, 760); // not really a good solution ...!
        lock.setKey(key);
        lock.setOwner(transaction.getName());
        lock.setOwnerStr(transaction.toString());
        long start = System.currentTimeMillis();
        while (true) {
            try {
                lock.save();
                return;
            } catch (MException e) {
                log().d(e);
            }
            // check age of lock entry
            try {
                DbLockObject obj = transaction.getDbManager().getObject(DbLockObject.class, key);
                if (obj != null && obj.getAge() > maxLockAge) {
                    log().i("remove stare lock", obj.getOwner(), obj.getOwnerStr(), key);
                    obj.delete();
                    continue;
                }
            } catch (MException e) {
                log().d(e);
            }

            if (System.currentTimeMillis() - start > timeout)
                throw new TimeoutRuntimeException(key);
            MThread.sleep(sleepTime);
        }
    }

    @Override
    public void releaseLock(Object object, String key, LockBase transaction) {
        try {
            DbLockObject obj = transaction.getDbManager().getObject(DbLockObject.class, key);
            if (obj != null) {
                if (obj.getOwner().equals(transaction.getName())) obj.delete();
                else {
                    log().w("you are not the lock owner", key, transaction);
                    if (ignoreLockOwner) obj.delete();
                }
            }
        } catch (Throwable e) {
            log().d(e);
        }
    }

    public void cleanup(DbManager manager) throws MException {
        for (DbLockObject o : manager.getAll(DbLockObject.class)) o.delete();
    }

    public long getMaxLockAge() {
        return maxLockAge;
    }

    public void setMaxLockAge(long maxLockAge) {
        this.maxLockAge = maxLockAge;
    }

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public boolean isLocked(Object object, String key, LockBase transaction) {
        try {
            DbLockObject obj = transaction.getDbManager().getObject(DbLockObject.class, key);
            if (obj != null && obj.getAge() > maxLockAge) {
                log().i("remove stare lock", obj.getOwner(), obj.getOwnerStr(), key);
                obj.delete();
                return false;
            }
            return obj != null;
        } catch (Throwable e) {
            log().d(e);
        }
        return false;
    }

    @Override
    public boolean isLockedByOwner(Object object, String key, LockBase transaction) {
        try {
            DbLockObject obj = transaction.getDbManager().getObject(DbLockObject.class, key);
            if (obj != null && obj.getAge() > maxLockAge) {
                log().i("remove stare lock", obj.getOwner(), obj.getOwnerStr(), key);
                obj.delete();
                return false;
            }
            return obj != null && obj.getOwner().equals(transaction.getName());
        } catch (Throwable e) {
            log().d(e);
        }
        return false;
    }
}
