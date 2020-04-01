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

import java.util.Date;

import de.mhus.lib.core.MDate;
import de.mhus.lib.core.logging.Log;
import de.mhus.lib.core.logging.MLogUtil;

public class ConnectionTrace implements Comparable<ConnectionTrace> {

    private StackTraceElement[] stackTrace;
    private long time;
    private long id;

    public ConnectionTrace(DbConnection con) {
        id = con.getInstanceId();
        time = System.currentTimeMillis();
        stackTrace = Thread.currentThread().getStackTrace();
    }

    @Override
    public String toString() {
        return MDate.toIsoDateTime(new Date(time));
    }

    //	public StackTraceElement[] getStackTrace() {
    //		return stackTrace;
    //	}

    @Override
    public int compareTo(ConnectionTrace o) {
        return Long.compare(time, o.time);
    }

    public void log(Log log) {
        log.w(id, "Connection", this);
        MLogUtil.logStackTrace(log, "" + id, stackTrace);
    }
}
