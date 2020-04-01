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
package de.mhus.lib.sql;

import de.mhus.lib.core.cfg.CfgTimeInterval;
import de.mhus.lib.core.lang.MObject;

public abstract class InternalDbConnection extends MObject implements DbConnection {

    protected static final CfgTimeInterval CFG_TIMEOUT_UNUSED =
            new CfgTimeInterval(DbConnection.class, "timeoutUnused", "10m");
    protected static final CfgTimeInterval CFG_TIMEOUT_LIFETIME =
            new CfgTimeInterval(DbConnection.class, "timeoutLifetime", "1h");

    protected DbPool pool;
    protected String poolId;
    protected long creationTime = 0;
    protected long lastUsedTime = 0;
    protected long timeoutUnused = CFG_TIMEOUT_UNUSED.interval();
    protected long timeoutLifetime = CFG_TIMEOUT_LIFETIME.interval();

    public InternalDbConnection() {
        creationTime = System.currentTimeMillis();
    }

    public void setPool(DbPool pool) {
        this.poolId = pool.getPoolId();
        this.pool = pool;
    }

    public boolean checkTimedOut() {
        if (isUsed()) return false;
        long currentTime = System.currentTimeMillis();
        if ((currentTime - creationTime > timeoutLifetime)
                || (lastUsedTime != 0 && currentTime - lastUsedTime > timeoutUnused)) {
            log().t("timeout");
            close();
            return true;
        }
        return false;
    }

    @Override
    public void setUsed(boolean used) {
        lastUsedTime = System.currentTimeMillis();
    }

    public long getTimeoutUnused() {
        return timeoutUnused;
    }

    public void setTimeoutUnused(long timeoutUnused) {
        this.timeoutUnused = timeoutUnused;
    }

    public long getTimeoutLifetime() {
        return timeoutLifetime;
    }

    public void setTimeoutLifetime(long timeoutLifetime) {
        this.timeoutLifetime = timeoutLifetime;
    }
}
