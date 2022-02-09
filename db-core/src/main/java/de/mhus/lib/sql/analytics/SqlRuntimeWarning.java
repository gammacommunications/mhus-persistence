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
package de.mhus.lib.sql.analytics;

import de.mhus.lib.core.MLog;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.core.cfg.CfgInitiator;
import de.mhus.lib.core.logging.MLogUtil;
import de.mhus.lib.core.mapi.IApiInternal;
import de.mhus.lib.core.mapi.MCfgManager;
import de.mhus.lib.core.node.INode;

public class SqlRuntimeWarning extends MLog implements SqlAnalyzer, CfgInitiator {

    private long traceMaxRuntime = MPeriod.MINUTE_IN_MILLISECOUNDS;

    @Override
    public void doAnalyze(
            long connectionId, String original, String query, long delta, Throwable t) {
        if (t != null) return;
        if (delta > traceMaxRuntime) {
            log().f("Query Runtime Warning", connectionId, delta, query);
            MLogUtil.logStackTrace(
                    log(), "" + connectionId, Thread.currentThread().getStackTrace());
        }
    }

    public long getTraceMaxRuntime() {
        return traceMaxRuntime;
    }

    public void setTraceMaxRuntime(long traceMaxRuntime) {
        this.traceMaxRuntime = traceMaxRuntime;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void doConfigure(INode config) {
        traceMaxRuntime = config.getLong("traceMaxRuntime", traceMaxRuntime);
    }

    @Override
    public void doInitialize(IApiInternal internal, MCfgManager manager, INode config) {
        if (config != null) doConfigure(config);
        SqlAnalytics.setAnalyzer(this);
    }
}
