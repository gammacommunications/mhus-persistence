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

import java.util.HashMap;

import de.mhus.lib.adb.Persistable;
import de.mhus.lib.core.MThread;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.errors.TimeoutRuntimeException;

public class MemoryLockStrategy extends LockStrategy {

    private long maxLockAge = MPeriod.HOUR_IN_MILLISECOUNDS;
    private long sleepTime = 200;

    private HashMap<String, LockObject> locks = new HashMap<>();

    @Override
    public void lock(Persistable object, String key, LockBase transaction, long timeout) {

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
            MThread.sleep(sleepTime);
        }
    }

    @Override
    public void releaseLock(Persistable object, String key, LockBase transaction) {
        synchronized (this) {
            LockObject obj = locks.get(key);
            if (obj == null) return;
            if (obj.owner.equals(transaction.getName())) locks.remove(key);
            else log().d("it's not lock owner", key, transaction);
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
