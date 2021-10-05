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

import java.util.HashMap;

import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.MThread;
import de.mhus.lib.core.cfg.CfgBoolean;
import de.mhus.lib.core.cfg.CfgLong;
import de.mhus.lib.errors.TimeoutRuntimeException;

public class MemoryLockStrategy extends LockStrategy {

    private static final CfgLong CFG_MAX_LOCK_AGE =
            new CfgLong(MemoryLockStrategy.class, "maxLockAge", MPeriod.HOUR_IN_MILLISECOUNDS);
    private static final CfgLong CFG_SLEEP_TIME =
            new CfgLong(MemoryLockStrategy.class, "sleepTime", 200);
    private static final CfgBoolean CFG_IGNORE_LOCK_OWNER =
            new CfgBoolean(MemoryLockStrategy.class, "ignoreLockOwner", false);

    private long maxLockAge = CFG_MAX_LOCK_AGE.value();
    private long sleepTime = CFG_SLEEP_TIME.value();
    private boolean ignoreLockOwner = CFG_IGNORE_LOCK_OWNER.value();

    private HashMap<String, LockObject> locks = new HashMap<>();

    @Override
    public boolean isLocked(Object object, String key, LockBase transaction) {
        synchronized (this) {
            LockObject current = locks.get(key);
            if (current != null && current.getAge() > maxLockAge) {
                log().i("remove old lock", current.owner, key);
                locks.remove(key);
                return false;
            }
            return current != null;
        }
    }

    @Override
    public boolean isLockedByOwner(Object object, String key, LockBase transaction) {
        synchronized (this) {
            LockObject current = locks.get(key);
            if (current != null && current.getAge() > maxLockAge) {
                log().i("remove old lock", current.owner, key);
                locks.remove(key);
                return false;
            }
            return current != null && current.owner.equals(transaction.getName());
        }
    }

    @Override
    public void lock(Object object, String key, LockBase transaction, long timeout) {

        long start = System.currentTimeMillis();
        while (true) {
            synchronized (this) {
                LockObject current = locks.get(key);
                if (current != null && current.getAge() > maxLockAge) {
                    log().i("remove old lock", current.owner, key);
                    locks.remove(key);
                    continue;
                }
                if (current == null) {
                    locks.put(key, new LockObject(transaction));
                    return;
                } else {
                    log().t("wait for lock", key);
                }
                //				if (current != null && current.equals(transaction)) {
                //					// already locked by me
                //					return;
                //				}
            }

            if (System.currentTimeMillis() - start > timeout)
                throw new TimeoutRuntimeException(key);
            MThread.sleepForSure(sleepTime);
        }
    }

    @Override
    public void releaseLock(Object object, String key, LockBase transaction) {
        synchronized (this) {
            LockObject obj = locks.get(key);
            if (obj == null) return;
            if (obj.owner.equals(transaction.getName())) locks.remove(key);
            else {
                log().w("it's not lock owner", key, obj.owner, transaction.getName());
                if (ignoreLockOwner) locks.remove(key);
            }
        }
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

    private class LockObject {
        public LockObject(LockBase transaction) {
            owner = transaction.getName();
        }

        public long getAge() {
            return System.currentTimeMillis() - created;
        }

        private long created = System.currentTimeMillis();
        private String owner;
    }
}
